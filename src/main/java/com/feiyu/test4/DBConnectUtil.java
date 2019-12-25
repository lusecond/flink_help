package com.feiyu.test4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: zzy
 * Date: 2019/5/28
 * Time: 8:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class DBConnectUtil {
    private static final Logger log = LoggerFactory.getLogger(DBConnectUtil.class);

    /**
     * 获取连接
     *
     * @param url
     * @param user
     * @param password
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(String url, String user, String password) throws SQLException {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.error("获取mysql.jdbc.Driver失败");
            e.printStackTrace();
        }
        try {
            conn = DriverManager.getConnection(url, user, password);
            log.info("获取连接:{} 成功...",conn);
        }catch (Exception e){
            log.error("获取连接失败，url:" + url + ",user:" + user);
        }

        return conn;
    }

    /**
     * 提交事物
     */
    public static void commit(Connection conn) {
        if (conn != null) {
            try {
                log.info("准备提交事务, 使用连接：{} ...",conn);
                conn.commit();
            } catch (SQLException e) {
                log.error("提交事务失败,Connection:" + conn);
                e.printStackTrace();
            } finally {
                close(conn);
            }
        }
    }

    /**
     * 事物回滚
     *
     * @param conn
     */
    public static void rollback(Connection conn) {
        if (conn != null) {
            try {
                log.info("事务回滚,Connection:" + conn);
                conn.rollback();
            } catch (SQLException e) {
                log.error("事务回滚失败,Connection:" + conn);
                e.printStackTrace();
            } finally {
                close(conn);
            }
        }
    }

    /**
     * 关闭连接
     *
     * @param conn
     */
    public static void close(Connection conn) {
        if (conn != null) {
            try {
                log.info("准备关闭连接:{} 成功...",conn);
                conn.close();
            } catch (SQLException e) {
                log.error("关闭连接失败,Connection:" + conn);
                e.printStackTrace();
            }
        }
    }
}
