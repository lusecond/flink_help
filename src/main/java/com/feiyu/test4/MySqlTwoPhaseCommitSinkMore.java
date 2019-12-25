package com.feiyu.test4;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: zzy
 * Date: 2019/5/28
 * Time: 8:47 PM
 * To change this template use File | Settings | File Templates.
 *
 * 自定义kafka to mysql,继承TwoPhaseCommitSinkFunction,实现两阶段提交
 */
public class MySqlTwoPhaseCommitSinkMore extends TwoPhaseCommitSinkFunction<List<ObjectNode>, Connection, Void> {

    private static final Logger log = LoggerFactory.getLogger(MySqlTwoPhaseCommitSinkMore.class);

    public MySqlTwoPhaseCommitSinkMore(){
        super(new KryoSerializer<>(Connection.class,new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected void invoke(Connection connection, List<ObjectNode> objectNodeList, Context context) throws Exception {
        String sql = "insert into `mysqlExactlyOnce_test` (`value`,`insert_time`) values (?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);

        objectNodeList.forEach(objectNode -> {
            String value = objectNode.get("value").toString();
            JSONObject valueJson = JSONObject.parseObject(value);
            String value_str = (String) valueJson.get("value");
            Timestamp value_time = new Timestamp(System.currentTimeMillis());
            try {

//                for (int index = 0; index < objectNode.size(); ++index) {
//                    this.ps.setObject(index + 1, objectNode.get(index));
//                }

                ps.setString(1,value_str);
                ps.setTimestamp(2,value_time);
                ps.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

        //执行insert语句
        ps.executeBatch();
        ps.close();
    }

    @Override
    protected Connection beginTransaction() throws Exception {
        log.info("start beginTransaction.......");
        String url = "jdbc:mysql://10.250.0.38:3306/xy_data_20027?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true&rewriteBatchedStatements=true";
        Connection connection = DBConnectUtil.getConnection(url, "public", "public");
        //设置手动提交
        connection.setAutoCommit(false);
        return connection;
    }

    @Override
    protected void preCommit(Connection connection) throws Exception {
        log.info("start preCommit...");
    }

    @Override
    protected void commit(Connection connection) {
        log.info("start commit...");
        DBConnectUtil.commit(connection);
    }

    @Override
    protected void abort(Connection connection) {
        log.info("start abort rollback...");
        DBConnectUtil.rollback(connection);
    }
}
