package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.impl.DefaultTairManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by iceke on 16/7/1.
 */
public class TairFactory {
    private static DefaultTairManager tairManager = null;
    private static final String CONFIG_ADDRESS1 = RaceConfig.TairConfigServer;
    private static final String CONFIG_ADDRESS2 = RaceConfig.TairSalveConfigServer;
    private static final String GROUP_ID = RaceConfig.TairGroup;
    public static final Integer TAIR_NAMESPACE = RaceConfig.TairNamespace;

    public static DefaultTairManager getInstance() {
        List<String> confServers = new ArrayList<String>();
        confServers.add(CONFIG_ADDRESS1);
        confServers.add(CONFIG_ADDRESS2);

        tairManager = new DefaultTairManager();
        tairManager.setConfigServerList(confServers);
        tairManager.setGroupName(GROUP_ID);
        tairManager.setTimeout(5000);
        tairManager.init();

        return tairManager;
    }

    public static void main(String[] args) {
        TairConnectPool pool = new TairConnectPool();
        pool.getConnection().put(1,"platformTaobao_1466902080","kzx");
        System.out.println(pool.getConnection().get(1, "platformTaobao_1466902080"));
        System.out.println(pool.getConnection().get(RaceConfig.TairNamespace, "ratio_1467488207"));
    }
}
