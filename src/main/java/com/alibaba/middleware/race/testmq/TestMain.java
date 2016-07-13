package com.alibaba.middleware.race.testmq;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by iceke on 16/7/7.
 */
public class TestMain {
    private transient LinkedBlockingDeque<String> queue;

    public void start(){
        queue = new LinkedBlockingDeque<String>(500);
    }
    public static void main(String args[]){
        TestMain a = new TestMain();
        a.start();
        a.queue.offer("a");
        //a.queue.offer("b");
        //a.queue.offer("c");
        System.out.println(a.queue.size());
        System.out.println(a.queue);
        try {
            String result = a.queue.take();
            System.out.println(a.queue);
        }catch (Exception e){

        }

    }
}
