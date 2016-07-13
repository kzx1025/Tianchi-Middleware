package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairFactory;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by iceke on 16/7/8.
 */
public class RatioReduceBolt implements IRichBolt {

    private transient static Logger LOG = LoggerFactory.getLogger(RatioReduceBolt.class);
    protected transient ConcurrentHashMap<Long, Double> pcMiniuteTrades;
    protected transient ConcurrentHashMap<Long, Double> mbMiniuteTrades;

    protected transient ConcurrentHashMap<String, Double> historyRecords;

    private transient LinkedBlockingDeque<Long> timeTagHistoryQueue;


    private OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        pcMiniuteTrades = new ConcurrentHashMap<Long, Double>();
        mbMiniuteTrades = new ConcurrentHashMap<Long, Double>();
        this.timeTagHistoryQueue = new LinkedBlockingDeque<Long>();

        historyRecords = new ConcurrentHashMap<String, Double>();
        new Thread(new RatioReduceDaemon()).start();

    }

    @Override
    public void execute(Tuple tuple) {
        try {
            long createTime = tuple.getLong(0);
            short platform = tuple.getShort(1);
            double totalPrice = tuple.getDouble(2);

            //LOG.info("!!!!!!RatioReduceBolt=====>createTime: " + createTime + ", totalPrice: " + totalPrice + " platform: " + platform);
            switch (platform){
                case 0:
                    //PC
                    Double pcMiniuteTrades = this.pcMiniuteTrades.get(createTime);
                    if(pcMiniuteTrades == null){
                        pcMiniuteTrades = 0.0;
                    }
                    pcMiniuteTrades += totalPrice;
                    this.pcMiniuteTrades.put(createTime,pcMiniuteTrades);
                    break;

                case 1:
                    //无线
                    Double mbMiniuteTrades = this.mbMiniuteTrades.get(createTime);
                    if(mbMiniuteTrades== null){
                        mbMiniuteTrades= 0.0;
                    }
                    mbMiniuteTrades += totalPrice;
                    this.mbMiniuteTrades.put(createTime,mbMiniuteTrades);
                    break;

                default:
                    break;
            }
            if(pcMiniuteTrades.containsKey(createTime)&&mbMiniuteTrades.containsKey(createTime)) {
                timeTagHistoryQueue.offer(createTime);
            }
            // ratioMemory.addValue(platform, createTime, totalPrice);

        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("!!!!!!StatRatioBolt=====> execute failed");
        }

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


    private class RatioReduceDaemon implements Runnable {


        private static final String PC_MINIUTE_TRADE_PREFIX = "pc_miniute_trade_prefix";
        private static final String MB_MINIUTE_TRADE_PREFIX = "mb_miniute_trade_prefix";
        //维护一个当前最大的时间戳,每一次对Tair 的更新操作,都需要更新到当前的时间戳
        private long currentMaxMiniuteTime;
        private DefaultTairManager tairOperator = TairFactory.getInstance();


        public void run() {
            LOG.info("!!!!!!StatRatioBolt=====>start to sychonize!!!");
            //将 ConcurrentHashMap 的结果相加,并存储到一个新的HashMap 中
            while(true) {
                Long timeTag = null;
                try {
                    timeTag = timeTagHistoryQueue.take();
                } catch (InterruptedException e) {
                }

                if (timeTag == null) {
                    continue;
                }
                if(pcMiniuteTrades.containsKey(timeTag) && mbMiniuteTrades.containsKey(timeTag)) {
                    updateAll(timeTag);
                }else{
                    timeTagHistoryQueue.offer(timeTag);
                }
            }
        }


        private List<Map.Entry<Long, Double>> getLadderSumFromMap(Map map) {
            List<Map.Entry<Long, Double>> forSortSumList = new ArrayList<Map.Entry<Long, Double>>(
                    map.entrySet()
            );

            //求符合条件的时间戳的list
            // List<Map.Entry<Long, Double>> requireList = new ArrayList<Map.Entry<Long, Double>>();


            //按时间戳排序
            Collections.sort(forSortSumList, new Comparator<Map.Entry<Long, Double>>() {
                @Override
                public int compare(Map.Entry<Long, Double> o1, Map.Entry<Long, Double> o2) {
                    return (o1.getKey().compareTo(o2.getKey()));
                }
            });
            //将结果相加
            if (forSortSumList.size() == 1) {
                currentMaxMiniuteTime = forSortSumList.get(0).getKey();
                return forSortSumList;

            }
            for (int i = 1; i < forSortSumList.size(); i++) {
                Map.Entry<Long, Double> preEntry = forSortSumList.get(i - 1);
                Map.Entry<Long, Double> curEntry = forSortSumList.get(i);
                curEntry.setValue(preEntry.getValue() + curEntry.getValue());
            }
            //更新当前最大的时间戳
            currentMaxMiniuteTime = forSortSumList.get(forSortSumList.size() - 1).getKey();
            return forSortSumList;
        }


        private void updateRecords(String prefix, long from, long to, double value) {
            StringBuilder sb = new StringBuilder();
            //LOG.info(sb.toString());
            String key;

            for (long i = from; i < to; i += 60) {
                try {
                    key = prefix + i;
                    //DataEntry entry = tairOperator.get(RaceConfig.TairNamespace,key).getValue();

                    //ResultCode rs  = tairOperator.put(RaceConfig.TairNamespace,key,newValue);
                    historyRecords.put(key, value);

                } catch (Exception e) {
                    LOG.error("!!!!!!StatRatioBolt=====>" + e.toString());
                }
            }
        }

        private void updateTrade(List<Map.Entry<Long, Double>> sortedLadderList, String prefix) {
            //下面将会执行一大堆的 Tair 操作
            if (sortedLadderList.size() == 1) {
                long from = sortedLadderList.get(0).getKey();
                long to = from;
                double value = sortedLadderList.get(0).getValue();
                updateRecords(prefix, from, to, value);
            }else {
                for (int i = 1; i < sortedLadderList.size(); i++) {
                    long from = sortedLadderList.get(i - 1).getKey();
                    long to = sortedLadderList.get(i).getKey();
                    double value = sortedLadderList.get(i - 1).getValue();
                    updateRecords(prefix, from, to, value);
                }
            }

            String key = prefix + Long.toString(sortedLadderList.get(sortedLadderList.size() - 1).getKey());
            double value = sortedLadderList.get(sortedLadderList.size() - 1).getValue();

            //ResultCode rs  = tairOperator.put(RaceConfig.TairNamespace,key,newValue);
            historyRecords.put(key, value);
        }

        //更新Tair 端的 pc 前一段时间的交易额
        private void updatePcTrade(List<Map.Entry<Long, Double>> sortedLadderList) {
            updateTrade(sortedLadderList, PC_MINIUTE_TRADE_PREFIX);
            //LOG.debug("更新Pc端每分钟的交易额成功");
        }

        //更新Tair 端的 mb 前一段时间的交易额
        private void updateMbTrade(List<Map.Entry<Long, Double>> sortedLadderList) {
            updateTrade(sortedLadderList, MB_MINIUTE_TRADE_PREFIX);
            //LOG.debug("更新Mb端每分钟的总交易额成功");
        }

        private void updateRatio(long fromTime, long toTime) {
            StringBuilder sb = new StringBuilder();
            sb.append("execute updateMbPcRatio,fromTime:").append(fromTime).append(" toTime:").append(toTime);
            long fat = (toTime - fromTime) / 60;
            sb.append("  need to update time num is " + fat);
            LOG.info(sb.toString());
            //更新 mbPcRatio 在时间段 [fromTime,toTime] 的值
            //逻辑是读 Tair 中的交易额,然后更新
            String pcKey, mbKey, ratioKey;
            Double pcValue, mbValue;

            for (long i = fromTime; i <= toTime; i += 60) {
                pcKey = PC_MINIUTE_TRADE_PREFIX + i;
                mbKey = MB_MINIUTE_TRADE_PREFIX + i;
                ratioKey = RaceConfig.prex_ratio + i;
                //读Tair 并更新相关的值
                mbValue = historyRecords.get(mbKey);
                pcValue = historyRecords.get(pcKey);
                if (mbValue == null || pcValue == null) {
                    //LOG.info("history get mbKey and pcKey is all null!!");
                    continue;
                }
                if (pcValue == 0) {
                    pcValue = 0.01;
                }
                double res = mbValue / pcValue;
                res = Math.rint(res * 100) / 100;
                sb = new StringBuilder();
                sb.append("!!!!!!StatRatioBolt=====>: put in Tair timestamp:").append(ratioKey).append(" ratio:").append(res);

                ResultCode rs = tairOperator.put(RaceConfig.TairNamespace,ratioKey,res);
                if (rs.isSuccess()) {
                    //LOG.info(sb.append("  success!!!!").toString());
                } else {
                    LOG.error(sb.append("  failed!!!!").toString());

                }

            }
        }

        private void updateAll(long currentTimeTag) {
            //将两个 hashmap 初始化,并更新Tair 中的数据
            //LOG.info("执行一次写Tair 工作,该工作将HashMap 中的交易额更新到Tair 中,并清空 HashMap");
            if (pcMiniuteTrades.size() == 0 && mbMiniuteTrades.size() == 0) {
                //LOG.info("!!!!!!StatRatioBolt=====>pcMap mbMap size is 0, no operation");
                return;
            }
            long pcMin = this.currentMaxMiniuteTime;
            long mbMin = this.currentMaxMiniuteTime;

            StringBuilder sb = new StringBuilder();
            sb.append("!!!!!!StatRatioBolt=====> before sychornize pcMiniuteTrades size: ").append(pcMiniuteTrades.size());
            //LOG.info(sb.toString());
            if (pcMiniuteTrades.size() != 0) {
                //不破坏对象的成员map 进行clone
                ConcurrentHashMap<Long,Double> tmpPcMap = new ConcurrentHashMap<Long, Double>();
                tmpPcMap.putAll(pcMiniuteTrades);
                List<Map.Entry<Long, Double>> pcSortedLadderSum = getLadderSumFromMap(tmpPcMap);

                updatePcTrade(pcSortedLadderSum);
                //pcMin = pcSortedLadderSum.get(0).getKey();
                pcMin = currentTimeTag;
            }


            if (mbMiniuteTrades.size() != 0) {
                ConcurrentHashMap<Long,Double> tmpMbMap = new ConcurrentHashMap<Long, Double>();
                tmpMbMap.putAll(mbMiniuteTrades);
                List<Map.Entry<Long, Double>> mbSortedLadderSum = getLadderSumFromMap(tmpMbMap);
                updateMbTrade(mbSortedLadderSum);
                //mbMin = mbSortedLadderSum.get(0).getKey();
                mbMin = currentTimeTag;
            }


            //获得所有时间戳最靠前的时间,目的是为了更新Tair 中存储的交易比例
            long currentMinMiniuteTime = pcMin < mbMin ? pcMin : mbMin;
            if(currentMinMiniuteTime == 0){
                currentMinMiniuteTime = pcMin > mbMin ? pcMin : mbMin;
            }
            sb.setLength(0);
            sb.append("start to update ratio!!!!!!");
            // LOG.info(sb.toString());
            updateRatio(currentMinMiniuteTime, currentMaxMiniuteTime);
        }
    }
}