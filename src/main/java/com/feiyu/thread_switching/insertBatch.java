package com.feiyu.thread_switching;

import com.alibaba.fastjson.JSONObject;
import com.feiyu.test4.MysqlExactlyOncePOJO;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.Callable;

public class insertBatch implements Callable<Connection> {

    private Connection connection;
    private List<MysqlExactlyOncePOJO> objectNodeList;

    public insertBatch(Connection connection, List<MysqlExactlyOncePOJO> objectNodeList) {
        this.connection = connection;
        this.objectNodeList = objectNodeList;
    }

    @Override
    public Connection call() throws Exception {
        System.out.println("insertBatch 线程ID：" + Thread.currentThread().getId());
        String sql = "insert into `mysqlExactlyOnce_test` (`value`,`insert_time`) values (?,?)";
        PreparedStatement ps = this.connection.prepareStatement(sql);

        for (MysqlExactlyOncePOJO t:this.objectNodeList) {
            ps.setObject(1,t.getValue());
            Timestamp value_time = new Timestamp(System.currentTimeMillis());
            ps.setObject(2,value_time);
            ps.addBatch();
        }

        //执行insert语句
        ps.executeBatch();
        ps.close();

        return this.connection;
    }
}
