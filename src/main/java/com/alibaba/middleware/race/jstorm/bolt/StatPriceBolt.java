package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairFactory;
import com.google.common.util.concurrent.AtomicDouble;
import com.taobao.tair.impl.DefaultTairManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by iceke on 16/7/6.
 */
public class StatPriceBolt implements IRichBolt {
    private transient static Logger LOG = LoggerFactory.getLogger(StatPriceBolt.class);

    private transient ConcurrentHashMap<String, AtomicDouble> paymentMap;
    private transient LinkedBlockingDeque<String> timeTagHistoryQueue;
    private OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.timeTagHistoryQueue = new LinkedBlockingDeque<String>();
        this.paymentMap = new ConcurrentHashMap<String, AtomicDouble>();

        this.collector = collector;

        StatPriceDaemon daemon = new StatPriceDaemon();

        new Thread(daemon).start();
        new Thread(daemon).start();

    }

    @Override
    public void execute(Tuple input) {
        String timeTag = input.getStringByField("time_tag");
        double value   = input.getDoubleByField("value");

        AtomicDouble oldValue = paymentMap.putIfAbsent(timeTag, new AtomicDouble(value));
        if(oldValue != null) {
            oldValue.addAndGet(value);
        }

        timeTagHistoryQueue.offer(timeTag);


    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    public class StatPriceDaemon implements Runnable {


        @Override
        public void run() {
            LOG.info("StatPriceDaemon start!!!");
            DefaultTairManager tairManager = TairFactory.getInstance();
            while(true) {
                String timeTag = null;
                try {
                    timeTag = timeTagHistoryQueue.take();
                } catch (InterruptedException e) {
                }

                if (timeTag == null) {
                    continue;
                }

              //  LOG.info("Payment put in tair ====>timeTag:"+ timeTag + " ,price:" + paymentMap.get(timeTag));
                tairManager.put( RaceConfig.TairNamespace, timeTag, paymentMap.get(timeTag).get());
            }
        }

    }
}
