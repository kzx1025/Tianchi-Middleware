package com.alibaba.middleware.race.model;

import java.io.Serializable;

/**
 * Created by iceke on 16/7/1.
 */
public class OrderBean implements Serializable{
    private static final long serialVersionUID = 2277714452693486955L;
    private double totalPrice;

    //2 or 3  (2 is Taobao,3 is TM)
    private short  platform;

    public OrderBean(double totalPrice, short platform){
        this.totalPrice = totalPrice;
        this.platform = platform;
    }

    public OrderBean(){

    }

    public void setPlatform(short paltform) {
        this.platform = paltform;
    }

    public void setTotalPrice(double totalPrice) {
        this.totalPrice = totalPrice;
    }

    public double getTotalPrice() {
        return totalPrice;
    }

    public short getPlatform() {
        return platform;
    }
}
