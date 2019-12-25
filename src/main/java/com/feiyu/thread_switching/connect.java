package com.feiyu.thread_switching;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.Callable;

public class connect implements Callable<Connection> {

    @Override
    public Connection call() throws Exception {
        System.out.println("connect 线程ID：" + Thread.currentThread().getId());
        String url = "jdbc:mysql://10.250.0.38:3306/xy_data_20027?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true&rewriteBatchedStatements=true";

        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("获取mysql.jdbc.Driver失败");
            e.printStackTrace();
        }

        try {
            connection = DriverManager.getConnection(url, "public", "public");
            System.out.println("获取连接 " + connection + "成功");
        }catch (Exception e){
            System.out.println("获取连接失败，url:" + url + ",user:" + "public");
        }

        //设置手动提交
//        connection.setAutoCommit(false);
        return connection;
    }
}
