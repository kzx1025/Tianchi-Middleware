package com.alibaba.middleware.race.jstorm.spout;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by iceke on 16/7/1.
 */
public class MetaTuple implements Serializable {
    private static final long serialVersionUID = 2277714452693486955L;
    private  AtomicInteger failureTimes;

    private  String msgId;  // notice

    private short platform;
    private double totalPrice;
    private long createTime;

    private transient CountDownLatch latch;
    private transient  boolean isSucess;

    public MetaTuple(String msgId, short platform, double totalPrice, long createTime) {
        this.msgId = msgId;

        this.platform = platform;
        this.totalPrice = totalPrice;
        this.createTime = createTime;
        this.isSucess = false;

        this.failureTimes = new AtomicInteger(0);

        this.latch = new CountDownLatch(1);
    }

    public MetaTuple(){

    }

    public AtomicInteger getFailureTimes() {
        return failureTimes;
    }

    public double getTotalPrice() {
        return totalPrice;
    }

    public long getCreateTime() {
        return createTime;
    }

    public short getPlatform() {
        return platform;
    }

    public String getMsgId() {
        return msgId;
    }

    public void done() {
        this.isSucess = true;
        latch.countDown();
    }

    public void fail() {
        isSucess = false;
        latch.countDown();
    }

    public boolean waitFinish() throws InterruptedException {
        return latch.await(4, TimeUnit.MINUTES);
    }

    public boolean isSucess() {
        return isSucess;
    }

    @Override
    public String toString() {
        return "MetaTuple{platform: " + this.platform + ", totalPrice: " + this.totalPrice +
                ", createTime: " + createTime + "}";
    }
}
