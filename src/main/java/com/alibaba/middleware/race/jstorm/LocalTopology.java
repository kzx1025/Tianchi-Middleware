package com.alibaba.middleware.race.jstorm;

import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.RecordBolt;
import com.alibaba.middleware.race.jstorm.spout.RocketSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by iceke on 16/6/29.
 * 用于本地cluster测试Topology
 */
public class LocalTopology {
    private static Logger LOG = LoggerFactory.getLogger(LocalTopology.class);

    private static String TOPOLOGY_NAME = RaceConfig.JstormTopologyName;

    private static final String TBORDER_SPOUT_ID = "TBOrderSpout";
    private static final String TMORDER_SPOUT_ID = "TMOrderSpout";
    private static final String PAYMENT_SPOUT_ID = "PaymentSpout";

    private static final String TM_RECORD_Plat_BOLT_ID = "TMOrderRecordBolt";
    private static final String TB_RECORD_Plat_BOLT_ID = "TBOrderRecordBolt";
    private static final String GEN_PAMENT_BOLT_ID = "GenPaymentBolt";

    private static String SPOUT_COMPONENT = "spout";
    private static String TRANSMIT_COMPONENT = "transmit";

    public static void main(String args[]){
        try {
            TopologyBuilder topologyBuilder = LocalTopology.createBuilder();
            submitTopology(topologyBuilder);
        }catch (Exception e){
            e.printStackTrace();
        }
    }



    private static void submitTopology(TopologyBuilder builder){
        try{
            LocalCluster localCluster = new LocalCluster();
            Map conf = new HashMap();
            localCluster.submitTopology(TOPOLOGY_NAME,conf,builder.createTopology());
            Thread.sleep(20000);
        }catch (Exception e){
            System.out.println("遇到了一些异常");
            System.out.println(e.getMessage()+ e.getClass());
        }
    }


    private static TopologyBuilder createBuilder() throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        //使用环境变量获得rocketmq nameserver 的值
        String key = "rocketmq.namesrv.addr";
        String nameServer = System.getProperty(key);

        if (nameServer == null) {
            nameServer = "211.69.198.208:9876";
        }

        RocketSpout spout = new RocketSpout("all", RaceConfig.MetaConsumerGroup, nameServer);
        RecordBolt recordBolt = new RecordBolt();

        builder.setSpout(SPOUT_COMPONENT, spout, 1);
        builder.setBolt(TRANSMIT_COMPONENT, recordBolt, 1)
                .fieldsGrouping(SPOUT_COMPONENT, new Fields("orderId"));


        /*RocketSpout tbOrderSpout = new RocketSpout(RaceConfig.MqTaobaoTradeTopic, RaceConfig.MetaConsumerGroup, nameServer);
        RocketSpout tmOrderSpout = new RocketSpout(RaceConfig.MqTmallTradeTopic, RaceConfig.MetaConsumerGroup, nameServer);

        RocketSpout paymentSpout = new RocketSpout(RaceConfig.MqPayTopic, RaceConfig.MetaConsumerGroup, nameServer);
        paymentSpout.setPullBatchSize(1);

        RecordPlatBolt tbOrderBolt = new RecordPlatBolt();
        RecordPlatBolt tmOrderBolt = new RecordPlatBolt();

        GenPaymentBolt genPaymentBolt = new GenPaymentBolt();

        builder.setSpout(TMORDER_SPOUT_ID, tmOrderSpout, 1);
        builder.setSpout(TBORDER_SPOUT_ID, tbOrderSpout, 1);

        builder.setSpout(PAYMENT_SPOUT_ID, paymentSpout, 1);

        builder.setBolt(TB_RECORD_Plat_BOLT_ID, tbOrderBolt, 1).shuffleGrouping(TBORDER_SPOUT_ID);
        builder.setBolt(TM_RECORD_Plat_BOLT_ID, tmOrderBolt, 1).shuffleGrouping(TMORDER_SPOUT_ID);


       builder.setBolt(GEN_PAMENT_BOLT_ID, genPaymentBolt, 1).shuffleGrouping(PAYMENT_SPOUT_ID);*/

        return builder;
    }
}
