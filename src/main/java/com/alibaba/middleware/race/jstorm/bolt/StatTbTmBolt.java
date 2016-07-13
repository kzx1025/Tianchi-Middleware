package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairConnectPool;
import com.alibaba.middleware.race.Tair.TairFactory;
import com.google.common.util.concurrent.AtomicDouble;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by iceke on 16/7/2.
 */
public class StatTbTmBolt implements IRichBolt {
    private transient static Logger LOG = LoggerFactory.getLogger(StatTbTmBolt.class);
    private transient ConcurrentHashMap<String, AtomicDouble> paymentMap;
    //private transient ConcurrentHashMap<Long, AtomicDouble> tmallMap;
    //阻塞队列
    private transient LinkedBlockingDeque<String> timeTagQueue;
    private   DefaultTairManager tairManager = null;

    private  HashSet<String> hashSet = null;

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.paymentMap = new ConcurrentHashMap<String, AtomicDouble>();
       // this.timeTagQueue = new LinkedBlockingDeque<String>(10000000);
        //this.tmallMap = new ConcurrentHashMap<Long, AtomicDouble>();
        //pool = new TairConnectPool();
        //tairManager = TairFactory.getInstance();
        //this.hashSet = new HashSet<String>();

        //new Thread(new CheckTair()).start();


        //启动线程
        StatTbTmBoltDaemon daemon = new StatTbTmBoltDaemon();
        new Thread(daemon).start();


    }

    @Override
    public void execute(Tuple tuple) {
        long createTime = tuple.getLong(0);
        short platform = tuple.getShort(1);
        double totalPrice = tuple.getDouble(2);
        String timeTag = null;

        if (platform == 2) {
            //taobao order
            //updateMap(taobaoMap,createTime,totalPrice,RaceConfig.prex_taobao,tairManager);
            //updateTair(createTime, totalPrice, RaceConfig.prex_taobao, tairManager);
            timeTag = RaceConfig.prex_taobao+Long.toString(createTime);
        } else if (platform == 3) {
            //tm order
            //updateMap(tmallMap,createTime,totalPrice,RaceConfig.prex_tmall,tairManager);
            //updateTair(createTime, totalPrice, RaceConfig.prex_tmall, tairManager);
            timeTag = RaceConfig.prex_tmall+Long.toString(createTime);
        } else {
            LOG.error("!!!!StatTbTmBolt receive unknown message,not tm or taobao");
            timeTag = "unknownPlatform";
        }
        //LOG.info("paymentMap add ");
        AtomicDouble oldValue = paymentMap.putIfAbsent(timeTag, new AtomicDouble(totalPrice));
        if(oldValue != null) {
            oldValue.addAndGet(totalPrice);
        }
        //新来的时间戳入队
        //timeTagQueue.offer(timeTag);
       // LOG.info("execute paymentMap size is"+paymentMap.size());

    }


    private void updateTair(long createTime, double totalPrice, String prefix, DefaultTairManager tairManager) {
        String timeTag = prefix + Long.toString(createTime);
        Result<DataEntry> rs = tairManager.get(RaceConfig.TairNamespace, timeTag);
        if (rs.isSuccess()) {
            DataEntry entry = rs.getValue();
            if (entry == null) {
                ResultCode rs1 = tairManager.put(RaceConfig.TairNamespace, timeTag, totalPrice);
                if (rs1.isSuccess()) {
                    LOG.info("!!!!StatTbTmBolt====> timeTag:" + timeTag + " not exist!!,so put first price:" + totalPrice);
                    // hashSet.add(timeTag);
                } else {
                    LOG.error("!!!!StatTbTmBolt====> timeTag:" + timeTag + " not exist!!,so put first price but failed!!!!!!!!");

                }

            } else {
                double previousPrice = (Double) entry.getValue();
                double nowPrice = previousPrice + totalPrice;
                ResultCode rs2 = tairManager.put(RaceConfig.TairNamespace, timeTag, nowPrice);
                if (rs2.isSuccess()) {
                    LOG.info("!!!!StatTbTmBolt====> timeTag:" + timeTag + "  exist!!,so put new price:" + nowPrice);
                    // hashSet.add(timeTag);
                } else {
                    LOG.error("!!!!StatTbTmBolt====> timeTag:" + timeTag + "  exist!!,so put new price but failed!!!!!!!!");
                }

            }
        } else {
            LOG.error("!!!!StatTbTmBolt====> updateTair failed!!");
        }
        //if(tairManager.get(RaceConfig.TairNamespace,timeTag))
    }

    private void updateMap(ConcurrentHashMap<Long, Double> map, long createTime, double totalPrice, String prefix, DefaultTairManager tairManager) {
        if (map.containsKey(createTime)) {
            double previousPrice = map.get(createTime);
            double nowPrice = previousPrice + totalPrice;

            map.put(createTime, nowPrice);
            LOG.info("!!!!!StatTbTmBolt updateMap===> ,createTime:" + createTime + " previousPrice:"
                    + previousPrice, " nowPrice:" + nowPrice);
        } else {
            LOG.info("!!!!!StatTbTmBolt updateMap==> createTime:" + createTime + "not exist,so put it");
            map.put(createTime, totalPrice);

        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RaceConfig.FIRST_STREAM_ID, new Fields("time_tag", "value"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }




    private class StatTbTmBoltDaemon implements Runnable {

        @Override
        public void run() {
            LOG.info("StatRbRmBoltDaemon start!!!");
            while (true) {
                try {
                    Thread.sleep(2000);
                } catch (Exception e) {
                    LOG.error("StatTbTmBoltDaemon sleep failed!!!");

                }

                if (paymentMap.size() <= 0) {
                    continue;
                }

                //LOG.info("paymentMap size is"+paymentMap.size());
                for (Map.Entry<String, AtomicDouble> entry : paymentMap.entrySet()) {

                    AtomicDouble totalValue = entry.getValue();

                    if (totalValue.get() > 0) { // 金额大于0
                        String timeTag = entry.getKey();
                        double price = entry.getValue().getAndSet(0);
                       // LOG.info("emit data===>timeTag:"+timeTag+", price:"+price);
                        collector.emit(RaceConfig.FIRST_STREAM_ID, new Values(timeTag,price));

                    }
                }


            }
        }
    }
}
