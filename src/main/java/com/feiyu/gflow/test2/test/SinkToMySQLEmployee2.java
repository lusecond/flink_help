package com.feiyu.gflow.test2.test;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

public class SinkToMySQLEmployee2 extends RichSinkFunction<List<Employee2>> {
    PreparedStatement ps;
    BasicDataSource dataSource;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
        String sql = "insert into employee(id, name, password, age, salary, department) values(?, ?, ?, ?, ?, ?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<Employee2> value, Context context) throws Exception {
        //遍历数据集合
        for (Employee2 model2 : value) {
            for (int i = 0; i < model2.productArity(); i++)
            {
                //                ps.setObject(i+1, model2.productElement(i));
                if (model2.productElement(i).getClass().getSimpleName() == "Integer") {
                    ps.setInt(i+1, (Integer) model2.productElement(i));
                } else {
                    ps.setString(i+1, model2.productElement(i).toString());
                }
            }
            ps.addBatch();
        }
        int[] count = ps.executeBatch();//批量后执行
        connection.commit();
        System.out.println("成功了插入了" + count.length + "行数据");
    }


    private static Connection getConnection(BasicDataSource dataSource) {
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setUrl("jdbc:mysql://10.250.0.38:3306/xy_data_20027?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true"); //test为数据库名
        dataSource.setUsername("public"); //数据库用户名
        dataSource.setPassword("public"); //数据库密码
        //设置连接池的一些参数
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            con.setAutoCommit(false);
            System.out.println("创建连接池：" + con);
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }

}
