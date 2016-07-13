package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    //这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_44803fzwqx_";
    public static String prex_taobao = "platformTaobao_44803fzwqx_";
    public static String prex_ratio = "ratio_44803fzwqx_";


    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String JstormTopologyName = "44803fzwqx";

    public static  String MetaConsumerGroup = "44803fzwqx";
    public static  String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static  String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static  String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";


    public static String TairConfigServer = "10.101.72.127:5198";
    public static String TairSalveConfigServer = "10.101.72.128:5198";
    public static String TairGroup = "group_tianchi";
    public static Integer TairNamespace = 23950;

    // RocketMQ配置
    public static int MQ_MAX_THREAD = 10;
    public static int MQ_MIN_THREAD = 8;
    public static String MQ_TAG = "*";
    public static String MQ_NAME_SERVER_URL = "211.69.198.208:9876";

    public static final boolean isCusomerFromTime = false;
    // spout配置
    public static boolean SPOUT_AUTO_ACK     = true;
    public static boolean SPOUT_FLOW_CONTROL = false;

    // streamId
    public static String FIRST_STREAM_ID = "first";
    public static String SECOND_STREAM_ID = "second";

    // tuple错误重试次数
    public static int DEFAULT_FAIL_TIME = 3;
}