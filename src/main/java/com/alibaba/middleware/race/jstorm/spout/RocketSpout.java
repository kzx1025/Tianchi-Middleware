package com.alibaba.middleware.race.jstorm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.metric.MetricClient;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.Pair;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by iceke on 16/6/29.
 */
public class RocketSpout implements IRichSpout, IAckValueSpout, IFailValueSpout,
        MessageListenerConcurrently {

    private static final long serialVersionUID = 8476906628618859716L;
    private static final Logger LOG = Logger.getLogger(RocketSpout.class);

    protected RocketClientConfig rocketClientConfig ;

    protected SpoutOutputCollector collector;
    protected transient DefaultMQPushConsumer consumer;

    protected Map conf;
    protected String id;
    protected boolean flowControl;
    protected boolean autoAck;

    protected transient LinkedBlockingDeque<Pair<MetaTuple, Long>> sendingQueue;

    protected transient MetricClient metricClient;
    protected transient AsmHistogram waithHistogram;
    protected transient AsmHistogram processHistogram;

    private final String rocketConsumeTopic;
    private final String rocketConsumeGroup;
    private final String nameServer;
    private final String subExp = "*";

    private volatile int taoBaoNum = 0;

    // 默认的 batchSize
    private int pullBatchSize = 32;

    public RocketSpout(String rocketConsumeTopic, String rocketConsumeGroup,String nameServer){
        this.rocketConsumeTopic = rocketConsumeTopic;
        this.rocketConsumeGroup = rocketConsumeGroup;
        this.nameServer = nameServer;

    }



    @Override
    public void fail(Object o, List<Object> list) {
        MetaTuple tuple = (MetaTuple) list.get(0);
        long orderId = (Long) list.get(1);
        AtomicInteger failTimes = tuple.getFailureTimes();
        int failNum = failTimes.incrementAndGet();
        if(failNum > RaceConfig.DEFAULT_FAIL_TIME) {
            finishTuple(tuple);
            return;
        }


        if(flowControl) {
            this.sendingQueue.offer(new Pair(tuple, orderId));
        } else {
            sendFirstTuple(tuple, orderId);
        }
    }

    public void finishTuple(MetaTuple metaTuple){
        metaTuple.done();
    }

    public void setPullBatchSize(int num){
        this.pullBatchSize = num;
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.conf = conf;
        this.collector = spoutOutputCollector;
        this.id = topologyContext.getThisComponentId() + ":" + topologyContext.getThisTaskId();
        this.sendingQueue = new LinkedBlockingDeque<Pair<MetaTuple, Long>>();

        this.flowControl = JStormUtils.parseBoolean(
                conf.get(RocketClientConfig.META_SPOUT_FLOW_CONTROL),RaceConfig.SPOUT_FLOW_CONTROL
        );
        this.autoAck = JStormUtils.parseBoolean(
                conf.get(RocketClientConfig.META_SPOUT_AUTO_ACK),RaceConfig.SPOUT_AUTO_ACK
        );

        StringBuilder sb = new StringBuilder();
        sb.append("Begin to init MetaSpout:").append(id);
        sb.append(",flowControl:").append(flowControl);
        sb.append(", autoAck:").append(autoAck);
        LOG.info(sb.toString());

        rocketClientConfig = new RocketClientConfig(this.rocketConsumeGroup,this.nameServer,this.rocketConsumeTopic
                ,this.subExp);

        //rocketClientConfig.setPullBatchSize(this.pullBatchSize);

        //使得consume 订阅相应的topic
        //如下代码代表rocketmq 的consumer 已经被创建,并注册了消费者函数
        try{
            consumer = RocketConsumerFactory.mkInstance(rocketClientConfig,this);
        }catch(Exception e){
            LOG.error("Failed to create Meta Consumer ",e);
            throw new RuntimeException("Failed to create RocketConsumer" + id,e);
        }

        if(consumer != null) {
            consumer.registerMessageListener(this);
            LOG.info("consumer:"+consumer.getInstanceName()+" ,"+consumer.toString());
            try {
                consumer.start();
            } catch (MQClientException e) {
                LOG.error("consumer start failed");
            }


        }else{
            LOG.warn(id + "already exist consumer in current worker, don't need to fetch data");
            //启动新的线程发送没有产生消息的信息
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true){
                        try{
                            Thread.sleep(10000);
                        }catch (InterruptedException e){
                            break;
                        }
                        StringBuilder sb = new StringBuilder();
                        sb.append("Only one spout consumer can be run on one process,");
                        sb.append(" but there are multiple spout consumes with the same topic@groupid meta, so the second one");
                        sb.append(id).append(" do nothing");
                        LOG.info(sb.toString());
                    }
                }
            }).start();
        }
        LOG.info("Successfully init " + id);
    }



    public void sendFirstTuple(MetaTuple tuple,long orderId){
        collector.emit(RaceConfig.FIRST_STREAM_ID, new Values(tuple, orderId), tuple.getMsgId());
    }

    public void sendSecondTuple(MetaTuple tuple,long orderId){
        collector.emit(RaceConfig.SECOND_STREAM_ID, new Values(tuple, orderId), tuple.getMsgId());
    }

    @Override
    public void nextTuple() {
        Pair<MetaTuple, Long> pair = null;
        try {
            // 阻塞
            // 顺便避免了空跑浪费CPU的问题
            pair = sendingQueue.take();
        } catch (InterruptedException e) {
            LOG.warn("error when take pair from sendingQueue");
        }

        if(pair == null) {
            return;
        }

        sendFirstTuple(pair.getObject1(), pair.getObject2());
    }



    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {

        try {
            for(MessageExt messageExt : list) {

                String msgId = messageExt.getMsgId();
                //LOG.info("messageId:"+msgId);

                MetaTuple tuple = null;

                byte[] body = messageExt.getBody();
                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                    //Info: 生产者停止生成数据, 并不意味着马上结束
                    System.out.println("Got the end signal");
                    continue;
                }
                String topic = messageExt.getTopic();
            /*
             * 0 - pc
             * 1 - mobile
             * 2 - taobao
             * 3 - tmall
             */
                long orderId = -1;
                if (topic.equals(RaceConfig.MqPayTopic)) {// payment
                    // LOG.info("!!!!!ConsumeMessage receive PaymentMessage");

                    PaymentMessage message = RaceUtils.readKryoObject(PaymentMessage.class, body);
                    tuple = new MetaTuple(
                            msgId,
                            message.getPayPlatform(),
                            message.getPayAmount(),
                            message.getCreateTime()
                    );
                    orderId = message.getOrderId();

                } else if (topic.equals(RaceConfig.MqTaobaoTradeTopic)) {
                    // LOG.info("!!!!ConsumeMessage receive TaobaolMessage");
                    OrderMessage taobaoMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                    tuple = new MetaTuple(
                            msgId,
                            (short) 2,
                            taobaoMessage.getTotalPrice(),
                            taobaoMessage.getCreateTime()
                    );
                    orderId = taobaoMessage.getOrderId();



                } else if (topic.equals(RaceConfig.MqTmallTradeTopic)) {
                    // LOG.info("!!!!!ConsumeMessage receive TmallMessage");
                    OrderMessage tmallMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                    tuple = new MetaTuple(
                            msgId,
                            (short) 3,
                            tmallMessage.getTotalPrice(),
                            tmallMessage.getCreateTime()
                    );
                    orderId = tmallMessage.getOrderId();


                } else {
                }


                //LOG.info("!!RocketSpout----put into Queue,orderId is:"+orderId);
                if (tuple != null) {
                    if (flowControl) {
                        sendingQueue.offer(new Pair(tuple, orderId));
                    } else {
                        //LOG.info("orderNum is:"+taoBaoNum);
                        //LOG.info(tuple.getMsgId());
                        sendFirstTuple(tuple, orderId);
                        if(topic.equals(RaceConfig.MqPayTopic)){
                            sendSecondTuple(tuple, orderId);
                        }
                    }


                }
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

        } catch (Exception e) {
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }

    }
    //获得consumer对象
    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }

    @Override
    public void ack(Object o, List<Object> list) {
        MetaTuple tuple = (MetaTuple) list.get(0);
        finishTuple(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(RaceConfig.FIRST_STREAM_ID,new Fields("MetaTuple", "orderId"));
        outputFieldsDeclarer.declareStream(RaceConfig.SECOND_STREAM_ID,new Fields("MetaTuple", "orderId"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    @Override
    public void ack(Object o) {
        LOG.warn("Shouldn't go this function");
    }

    @Override
    public void fail(Object o) {
        LOG.warn("Shouldn't go this function");
    }

    @Override
    public void close() {
        if(consumer !=null){
            consumer.shutdown();
        }
    }

    @Override
    public void activate() {
        if(consumer != null){
            consumer.resume();
        }
    }

    @Override
    public void deactivate() {
        if(consumer != null){
            consumer.suspend();
        }
    }

}
