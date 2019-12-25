package com.feiyu.thread_switching;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public class close implements Callable<Integer> {

    private Connection connection;

    public close(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Integer call() throws Exception {
        System.out.println("close 线程ID：" + Thread.currentThread().getId());
        if (this.connection != null) {
            try {
                System.out.println("准备关闭连接, 使用连接：" + this.connection);
                this.connection.close();
            } catch (SQLException e) {
                System.out.println("关闭连接失败, 使用连接：" + this.connection);
                e.printStackTrace();
            }
        }
        return 0;
    }
}
