package com.alibaba.middleware.race.jstorm.spout;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import org.apache.log4j.Logger;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;


/**
 * Created by iceke on 16/6/29.
 * 消费者工厂,用于产生rocket消费者的实例
 */
public class RocketConsumerFactory {

    private static final Logger LOG = Logger.getLogger(RocketConsumerFactory.class);


    public static volatile HashMap<String, DefaultMQPushConsumer> consumerCache = new HashMap<String, DefaultMQPushConsumer>();

    public static synchronized DefaultMQPushConsumer mkInstance(RocketClientConfig config,
                                                                MessageListenerConcurrently listener) throws Exception {
        DefaultMQPushConsumer consumer = consumerCache.get(RaceConfig.MetaConsumerGroup);
        if (consumer != null) {
            //Attention, this place return null to info duplicated consumer
            return null;
        } else {
        }

        consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
        consumer.setInstanceName(RaceConfig.MetaConsumerGroup + JStormUtils.process_pid());

        String key = "rocketmq.namesrv.addr";
        String nameServer = System.getProperty(key);

        if (nameServer == null) {
            nameServer =  RaceConfig.MQ_NAME_SERVER_URL;
        }
       // consumer.setNamesrvAddr(nameServer);

        try {
            consumer.subscribe(RaceConfig.MqPayTopic , RaceConfig.MQ_TAG);
            consumer.subscribe(RaceConfig.MqTmallTradeTopic , RaceConfig.MQ_TAG);
            consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, RaceConfig.MQ_TAG);
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        consumer.setPullBatchSize(32);
        consumer.setConsumeMessageBatchMaxSize(32);

        //consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumerCache.put(RaceConfig.MetaConsumerGroup, consumer);
        return consumer;
    }


}
