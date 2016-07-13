package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.spout.MetaTuple;
import com.alibaba.rocketmq.common.Pair;
import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by iceke on 16/7/7.
 */
public class RatioBolt implements IRichBolt {

    private transient static Logger LOG = LoggerFactory.getLogger(RatioBolt.class);

    private static final String PC_MINIUTE_TRADE_PREFIX = "pc_miniute_trade_prefix";
    private static final String MB_MINIUTE_TRADE_PREFIX = "mb_miniute_trade_prefix";
    protected transient ConcurrentHashMap<Long, AtomicDouble> pcMap;
    protected transient ConcurrentHashMap<Long, AtomicDouble> mbMap;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.pcMap = new ConcurrentHashMap<Long, AtomicDouble>();
        this.mbMap = new ConcurrentHashMap<Long, AtomicDouble>();
        this.collector = collector;

        //启动线程
        RatioBoltDaemon daemon = new RatioBoltDaemon();
        new Thread(daemon).start();
    }

    @Override
    public void execute(Tuple tuple) {
        Pair<MetaTuple, Long> pair = new Pair<MetaTuple, Long>((MetaTuple) tuple.getValueByField("MetaTuple"), tuple.getLongByField("orderId"));
        MetaTuple metaTuple = (MetaTuple) pair.getObject1();
        long createTime = (metaTuple.getCreateTime() / 1000 / 60) * 60;
        short platform = metaTuple.getPlatform();
        double totalPrice = metaTuple.getTotalPrice();

        ConcurrentHashMap<Long, AtomicDouble> map = null;
        switch (platform) {
            case 0:
                map = this.pcMap;
                break;
            case 1:
                map = this.mbMap;
                break;
        }

        // update map
        AtomicDouble oldValue = map.putIfAbsent(createTime, new AtomicDouble(totalPrice));
        if(oldValue != null) {
            oldValue.addAndGet(totalPrice);
        }


    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RaceConfig.SECOND_STREAM_ID, new Fields("create_time","platform", "price"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private class RatioBoltDaemon implements Runnable {

        @Override
        public void run() {

            while(true) {
                // 维护taobao和tmall的map
                // 定时刷新到下一集汇总bolt中去
                try {
                    Thread.sleep(8000); // sleep 1s
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                flush(pcMap,(short)0);
                flush(mbMap,(short)1);
            }

        }

        public void flush(ConcurrentHashMap<Long, AtomicDouble> map, short platform) {
            if(map.size() <= 0) {
                return;
            }
            //对map排序
            List<Map.Entry<Long, AtomicDouble>> sortList = new ArrayList<Map.Entry<Long, AtomicDouble>>(
                    map.entrySet()
            );
            Collections.sort(sortList, new Comparator<Map.Entry<Long, AtomicDouble>>() {
                @Override
                public int compare(Map.Entry<Long, AtomicDouble> o1, Map.Entry<Long, AtomicDouble> o2) {
                    return (o1.getKey().compareTo(o2.getKey()));
                }
            });

            for(Map.Entry<Long, AtomicDouble> entry : map.entrySet()) {

                double price = entry.getValue().getAndSet(0);
                long createTime = entry.getKey();

                if(price > 0) {
                    collector.emit(RaceConfig.SECOND_STREAM_ID, new Values( createTime,platform, price));
                   // LOG.info("emit createTIme:"+createTime+" platform:"+platform+" price:"+price);
                }
            }
        }
    }
}
