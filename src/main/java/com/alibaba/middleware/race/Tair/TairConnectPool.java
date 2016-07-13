package com.alibaba.middleware.race.Tair;

import com.taobao.tair.TairManager;
import com.taobao.tair.impl.DefaultTairManager;


import java.util.Vector;

/**
 * Created by ckboss on 16-6-26.
 */
public class TairConnectPool {

    class PooledConnection {
        private DefaultTairManager tairManager = null;
        private volatile boolean isOK ;

        PooledConnection(DefaultTairManager defaultTairManager) {
            this.tairManager = defaultTairManager;
            this.isOK = true;
        }

        public DefaultTairManager getTairManager() {
            return tairManager;
        }

        public void setTairManager(DefaultTairManager tairManager) {
            this.tairManager = tairManager;
        }

        public boolean isOK() {
            return isOK;
        }

        public void setOK(boolean OK) {
            isOK = OK;
        }
    }

    private Vector<PooledConnection> collections = null;
    private int maxConn = 20;
    private int minConn = 5;
    private int RETRY_TIME = 5;

    public TairConnectPool() {
    }

    public TairConnectPool(int maxConn) {
        maxConn = Math.max(maxConn,this.minConn);
        this.maxConn = maxConn;
    }

    public TairConnectPool(int minConn, int maxConn) {
        if(minConn>maxConn) {
            maxConn ^= minConn;
            minConn ^= maxConn;
            maxConn ^= minConn;
        }
        this.minConn = minConn;
        this.maxConn = maxConn;
    }

    public synchronized void CreatePool() {
        if(this.collections == null) {
            this.collections = new Vector<PooledConnection>();
        }
        while(this.collections.size()<this.minConn) {
            PooledConnection pooledConnection = new PooledConnection(TairFactory.getInstance());
            this.collections.add(pooledConnection);
        }
    }

    public synchronized DefaultTairManager getConnection() {

        if(this.collections == null) {
            this.CreatePool();
        }

        // try
        int cnt=this.RETRY_TIME;
        while(cnt-->0) {
            DefaultTairManager tairManager = this.getFreeTailManager();
            if(tairManager==null) {
                this.waitMillis(100);
            } else {
                return tairManager;
            }
        }

        return null;
    }

    private synchronized DefaultTairManager getFreeTailManager() {
        // find free tairmaager first
        for(int i=0,sz=this.collections.size();i<sz;i++) {
            PooledConnection pooledConnection = this.collections.get(i);
            if(pooledConnection.isOK()==true) {
                // find a free one
                pooledConnection.setOK(false);
                return pooledConnection.getTairManager();
            }
        }

        // can't find try to build a new tairmanager
        if(this.collections.size()<this.maxConn) {
            PooledConnection pooledConnection = new PooledConnection(TairFactory.getInstance());
            this.collections.add(pooledConnection);
            return getFreeTailManager();
        }

        // can't find a free tairmanager
        throw new NullPointerException("can't creat a new TairManager !!");
    }

    public synchronized void CloseConnectPool() {

        if(this.collections==null) {
            return ;
        }

        for(int i=0,sz=this.collections.size();i<sz;i++) {
            PooledConnection pooledConnection = this.collections.get(i);
            pooledConnection.getTairManager().close();
        }

        this.collections.clear();
    }

    public synchronized void releaseConn(TairManager tairManager) {

        for(PooledConnection pooledConnection : this.collections) {
            if(pooledConnection.getTairManager().equals(tairManager)) {
                pooledConnection.setOK(true);
            }
        }
    }

    private void waitMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
