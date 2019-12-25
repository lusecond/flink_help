package com.feiyu.thread_switching;

import java.sql.Connection;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class t {
    public static void main(String[] args) {
        try {
            System.out.println("main线程ID：" + Thread.currentThread().getId());

            Thread thread=new Thread(new connect2());
            thread.start();


//            thread.run();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
