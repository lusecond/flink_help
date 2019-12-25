package com.feiyu.thread_switching;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public class commit implements Callable<Connection> {

    private Connection connection;

    public commit(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Connection call() throws Exception {
        System.out.println("commit 线程ID：" + Thread.currentThread().getId());
        if (this.connection != null) {
            try {
                System.out.println("准备提交事务, 使用连接：" + this.connection);
                this.connection.commit();
            } catch (SQLException e) {
                System.out.println("提交事务失败, 使用连接：" + this.connection);
                e.printStackTrace();
            }
        }
        return this.connection;
    }
}
