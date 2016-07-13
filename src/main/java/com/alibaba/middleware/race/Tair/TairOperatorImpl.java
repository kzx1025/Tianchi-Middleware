package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {

    private DefaultTairManager tairManager ;
    private int namespace;

    //constructor
    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
        List<String> confServer = new ArrayList<String>();
        confServer.add(masterConfigServer);

        tairManager = new DefaultTairManager();
        tairManager.setConfigServerList(confServer);

        tairManager.setGroupName(groupName);
        tairManager.init();
        this.namespace = namespace;
    }

    public boolean write(Serializable key, Serializable value) {
        ResultCode rc = tairManager.put(namespace,key,value);
        return rc.isSuccess();
    }

    public DataEntry get(Serializable key) {
        Result<DataEntry> result = tairManager.get(namespace,key);
        if(result.isSuccess()){
            DataEntry entry = result.getValue();
            return entry;
        }
        else {
            return null;
        }
    }

    public boolean remove(Serializable key) {

        ResultCode rc = tairManager.delete(namespace,key);
        return rc.isSuccess();
    }



    public void close(){
        //TODO
    }

    //天猫的分钟交易额写入tair
    public static void main(String [] args) throws Exception {
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        /*
        //假设这是付款时间
        Long millisTime = System.currentTimeMillis();
        //由于整分时间戳是10位数，所以需要转换成整分时间戳
        Long minuteTime = (millisTime / 1000 / 60) * 60;
        //假设这一分钟的交易额是100;
        Double money = 100.0;
        //写入tair
        tairOperator.write(RaceConfig.prex_tmall + minuteTime, money);
        */
        tairOperator.write("test","test_result");
        Object a = tairOperator.get("test");
        Object ob1 =  tairOperator.get("1467340327900");

        Object b = tairOperator.get("1467341731048");
        System.out.println(b);



        System.out.println(ob1);

    }
}
