package com.alibaba.middleware.race.jstorm.spout.testSpout;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.jstorm.spout.RocketTuple;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by iceke on 16/6/29.
 */
public class ExampleBolt implements IRichBolt {
    private static final long serialVersionUID = 2495121976857546346L;

    private static final Logger LOG = LoggerFactory.getLogger(ExampleBolt.class);
    protected OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
        this.collector = collector;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void execute(Tuple tuple) {

        RocketTuple rocketTuple = (RocketTuple)tuple.getValue(0);

        try{
            System.out.println("从Rocket获得一个tuple");
            List<MessageExt> msgs = rocketTuple.getMsgList();
            for(MessageExt msg : msgs){
                    byte [] body = msg.getBody();
                    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                        continue;
                    }

                    PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                    System.out.println(paymentMessage);
            }
            System.out.println(rocketTuple.toString());
        }catch (Exception e){
            collector.fail(tuple);
            return;
        }
        collector.ack(tuple);
    }
}
