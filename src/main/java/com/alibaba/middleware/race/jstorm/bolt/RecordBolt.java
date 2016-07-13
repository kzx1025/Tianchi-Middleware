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
import com.alibaba.middleware.race.model.OrderBean;
import com.alibaba.rocketmq.common.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by iceke on 16/7/1.
 */
public class RecordBolt implements IRichBolt {
    private transient static Logger LOG = LoggerFactory.getLogger(RecordBolt.class);
    private transient volatile LinkedBlockingDeque<Pair<MetaTuple, Long>> messageQueue;
    private transient static ConcurrentHashMap<Long, OrderBean> orderRecords;
    private transient static ConcurrentHashMap<String, Short> allMap;

    private int INDEX_CAPACITY = 14000000;

    private static final long TOTAL_MESSAGE_NUM = 11000000;

    private double taobaoAmount1 = 0;
    private double taobaoAmount2 = 0;
    private int taobaoNum = 0;
    private transient long messageNum = 0;
    private volatile long emitNum = 0;
    private RecordBoltDaemon recordBoltDaemon;


    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        orderRecords = new ConcurrentHashMap<Long, OrderBean>(this.INDEX_CAPACITY);
        allMap = new ConcurrentHashMap<String, Short>(17000000);
        this.messageQueue = new LinkedBlockingDeque<Pair<MetaTuple, Long>>(this.INDEX_CAPACITY);

        this.collector = outputCollector;
        new Thread(new RecordBoltDaemon()).start();
        //new Thread(recordDaemon).start();
    }

    @Override
    public void execute(Tuple tuple) {
        Pair<MetaTuple, Long> pair = new Pair<MetaTuple, Long>((MetaTuple) tuple.getValueByField("MetaTuple"), tuple.getLongByField("orderId"));
        /** LOG.info("receive tuple,createTime:"+((MetaTuple) pair.getObject1()).getCreateTime());
         // LOG.info("!!!!!!!!!!ReocrdBolt: receive tuple,orderId:" + pair.getObject2());
         if(pair.getObject1().getPlatform() == 2){
         taobaoAmount1+=pair.getObject1().getTotalPrice();
         taobaoNum+=1;

         }**/
        // messageNum++;
        //LOG.info("messageNum:"+messageNum);
        MetaTuple metaTuple = (MetaTuple) pair.getObject1();
        Long orderId = (Long) pair.getObject2();
        short platform = metaTuple.getPlatform();
        String messageId = metaTuple.getMsgId();
       // LOG.info("messageId is "+ messageId);
        if (allMap.containsKey(messageId)) {
            //重复的message
            return;
        }
        if (platform == 2 || platform == 3) {
            double totalPrice = metaTuple.getTotalPrice();
            orderRecords.put(orderId, new OrderBean(totalPrice, platform));
        } else {
            //payment into queue
            messageQueue.offer(pair);
        }

        allMap.put(messageId, (short) 1);

    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(RaceConfig.FIRST_STREAM_ID, new Fields("createTime", "platform", "totalPrice")); // 第一题
        // outputFieldsDeclarer.declareStream(RaceConfig.SECOND_STREAM_ID, new Fields("createTime", "platform", "totalPrice")); // 第二题

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    private class RecordBoltDaemon implements Runnable {

        @Override
        public void run() {
            LOG.info("Record Daemon start!!");

            LOG.info("!!!!!!!!!!!!messageQueue:" + messageQueue.size());
            //bianli payment queue
            while (true) {
                Pair pair = null;
                MetaTuple metaTuple = null;
                long orderId = -1;
                try {
                    pair = messageQueue.take();
                    //LOG.info("!!!!!RecordBolt: thread start to process orderId:"+orderId);
                } catch (InterruptedException e) {
                    //LOG.error("!!!!!RecordBolt: thread ==>messageQueue get pair failed!!!!");
                }

                if (pair == null) {
                    continue;
                }


                metaTuple = (MetaTuple) pair.getObject1();
                orderId = (Long) pair.getObject2();

                double totalPrice = metaTuple.getTotalPrice();
                short platform = metaTuple.getPlatform();

                // it is PaymentMessage
                //LOG.info("!!!!!RecordBolt: this is payment Message!!");
                OrderBean orderBean = null;
                try {
                    orderBean = orderRecords.get(orderId);
                } catch (Exception e) {
                }
                if (orderBean == null) {
                    //orderMessage不存在,paymentMessage继续入队
                    //LOG.info("!!!!!RecordBolt:  payment's orderId don't exists in orderMap,pair into queue");
                    messageQueue.offer(pair);
                    continue;
                }

                long createTime = (metaTuple.getCreateTime() / 1000 / 60) * 60;
                // LOG.info("createTime: previous value:"+metaTuple.getCreateTime() / 1000+" nowTime:"+createTime);
                // 获取payment的价格，在totalPrice上减去
                double remainPrice = orderBean.getTotalPrice() - totalPrice;
                orderBean.setTotalPrice(remainPrice);
                if (orderBean.getTotalPrice() <= 0.01) {
                    //如果Order对应的Payment发送完毕，则remove该条数据
                    //LOG.info("!!!!!RecordBolt: orderBean's price is 0,so remove it from orderRecords");
                    orderRecords.remove(orderId);
                } else {
                    //如果Order对应的Payment没发送完毕，则重新覆盖orderBean
                    // LOG.info("!!!!!RecordBolt: orderBean's price remains, so update orderRecords");
                    orderRecords.put(orderId, orderBean);
                }

                short taobaoOrTMPlatform = orderBean.getPlatform();
                /**if(taobaoOrTMPlatform == 2){
                 taobaoAmount2+=totalPrice;
                 }

                 LOG.info("taobaoAmount1 total is "+taobaoAmount1+"taobaoAmount2 total is "+taobaoAmount2+"taobaoNum total is "+taobaoNum);**/

                // emitNum++;
                //LOG.info("emitNum:"+emitNum);
                collector.emit(RaceConfig.FIRST_STREAM_ID, new Values(createTime, taobaoOrTMPlatform, totalPrice)); // 第一题
                //  collector.emit(RaceConfig.SECOND_STREAM_ID, new Values(createTime, pcOrMobilePlatform, totalPrice)); // 第二题
            }
        }
    }


}
