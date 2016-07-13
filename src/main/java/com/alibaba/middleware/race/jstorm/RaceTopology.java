package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.*;
import com.alibaba.middleware.race.jstorm.spout.RocketSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);
    private static String TOPOLOGY_NAME = RaceConfig.JstormTopologyName;

    private static final String TBORDER_SPOUT_ID = "TBOrderSpout";
    private static final String TMORDER_SPOUT_ID = "TMOrderSpout";
    private static final String PAYMENT_SPOUT_ID = "PaymentSpout";

    private static final String TM_RECORD_Plat_BOLT_ID = "TMOrderRecordBolt";
    private static final String TB_RECORD_Plat_BOLT_ID = "TBOrderRecordBolt";
    private static final String GEN_PAMENT_BOLT_ID = "GenPaymentBolt";

    private static final String SPOUT_COMPONENT = "rocket_spout";
    private static final String CLASSIFY_BOLT = "classify_bolt";
    private static final String RECORD_BOLT = "record_bolt";
    private static final String STAT_TBTM_BOLT = "stat_tb_tm_bolt";
    private static final String STAT_PRICE_BOLT = "stat_price_bolt";
    private static final String STAT_RATIO_BOLT = "stat_ratio_bolt";
    private static final String RATIO_BOLT = "ratio_bolt";
    private static final String RATIO_REDUCE_BOLT = "ratio_reduce_bolt";


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = createBuilder();
        Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setNumAckers(0);
        //Map conf = new HashMap();

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static TopologyBuilder createBuilder() throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        //使用环境变量获得rocketmq nameserver 的值
        String key = "rocketmq.namesrv.addr";
        String nameServer = System.getProperty(key);

        if (nameServer == null) {
            nameServer = RaceConfig.MQ_NAME_SERVER_URL;
        }

        RocketSpout spout = new RocketSpout("all", RaceConfig.MetaConsumerGroup, nameServer);
        RecordBolt recordBolt = new RecordBolt();
        StatTbTmBolt statTbTmBolt = new StatTbTmBolt();
        StatPriceBolt statPriceBolt = new StatPriceBolt();
        RatioBolt ratioBolt = new RatioBolt();
        RatioReduceBolt ratioReduceBolt = new RatioReduceBolt();


        builder.setSpout(SPOUT_COMPONENT, spout, 4);

        builder.setBolt(RECORD_BOLT, recordBolt, 12)
                .fieldsGrouping(SPOUT_COMPONENT,RaceConfig.FIRST_STREAM_ID, new Fields("orderId"));
        builder.setBolt(STAT_TBTM_BOLT, statTbTmBolt, 12)
                .fieldsGrouping(RECORD_BOLT, RaceConfig.FIRST_STREAM_ID, new Fields("platform"));
        builder.setBolt(STAT_PRICE_BOLT, statPriceBolt, 1)
                .globalGrouping(STAT_TBTM_BOLT, RaceConfig.FIRST_STREAM_ID);

        builder.setBolt(RATIO_BOLT, ratioBolt, 12).fieldsGrouping(SPOUT_COMPONENT,RaceConfig.SECOND_STREAM_ID, new Fields("orderId"));
        builder.setBolt(RATIO_REDUCE_BOLT, ratioReduceBolt, 1)
                .globalGrouping(RATIO_BOLT, RaceConfig.SECOND_STREAM_ID);

        return builder;
    }



}