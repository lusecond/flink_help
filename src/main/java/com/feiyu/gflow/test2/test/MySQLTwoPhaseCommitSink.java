package com.feiyu.gflow.test2.test;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;
import java.util.List;

/**
 * 实现两阶段提交MySQL
 */
public class MySQLTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<List<Row>, Connection, Void>  implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(MySQLTwoPhaseCommitSink.class);

    private String drivername;
    private String url;
    private String username;
    private String password;
    private String sql;
    private int[] types;

    private PreparedStatement prepareStatement;

    public MySQLTwoPhaseCommitSink(String url, String drivername, String username, String password, String sql, int[] types) {
        super(new KryoSerializer<>(Connection.class,new ExecutionConfig()), VoidSerializer.INSTANCE);

        this.drivername = drivername;
        this.url = url;
        this.username = username;
        this.password = password;
        this.sql = sql;
        this.types = types;
    }

    /**
     * 执行数据库入库操作  task初始化的时候调用
     *
     * @param connection
     * @param data
     * @param context
     * @throws Exception
     */
    @Override
    protected void invoke(Connection connection, List<Row> data, Context context) throws Exception {
        log.info("start invoke...");

        data.forEach(row -> {
            try {
                this.writeToPreparedStatement(row);
            } catch (IOException ex) {

            }
        });

        this.flush();
    }

    void flush() {
        try {
            this.prepareStatement.executeBatch();
        } catch (SQLException var2) {
            throw new RuntimeException("Execution of JDBC statement failed.", var2);
        }
    }

    /**
     * 获取连接，开启手动提交事物（getConnection方法中）
     *
     * @return
     * @throws Exception
     */
    @Override
    protected Connection beginTransaction() throws Exception {
        log.info("start beginTransaction.......");

        Connection connection = null;

        try {
            connection = this.establishConnection();
            this.prepareStatement = connection.prepareStatement(this.sql);
        } catch (SQLException var4) {
            throw new IllegalArgumentException("open() failed.", var4);
        } catch (ClassNotFoundException var5) {
            throw new IllegalArgumentException("JDBC driver class not found.", var5);
        }

        // 设置手动提交
        connection.setAutoCommit(false);

        return connection;
    }

    private Connection establishConnection() throws SQLException, ClassNotFoundException {
        Class.forName(this.drivername);
        if (this.username == null) {
            return DriverManager.getConnection(this.url);
        } else {
            return DriverManager.getConnection(this.url, this.username, this.password);
        }
    }

    /**
     * 预提交，这里预提交的逻辑在invoke方法中
     *
     * @param connection
     * @throws Exception
     */
    @Override
    protected void preCommit(Connection connection) throws Exception {
        log.info("start preCommit...");
    }

    /**
     * 如果invoke方法执行正常，则提交事务
     *
     * @param connection
     */
    @Override
    protected void commit(Connection connection) {
        log.info("start commit...");
        if (connection != null) {
            try {
                connection.commit();
            } catch (SQLException e) {
                log.error("提交事物失败,Connection:" + connection);
                e.printStackTrace();
            } finally {
                close(connection);
            }
        }
    }

    @Override
    protected void recoverAndCommit(Connection connection) {
        log.info("start recoverAndCommit......."+connection);

    }


    @Override
    protected void recoverAndAbort(Connection connection) {
        log.info("start abort recoverAndAbort......."+connection);
    }

    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     *
     * @param connection
     */
    @Override
    protected void abort(Connection connection) {
        log.info("start abort rollback...");
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                log.error("事物回滚失败,Connection:" + connection);
            } finally {
                close(connection);
            }
        }
    }

    /**
     * 关闭连接
     *
     * @param connection
     */
    public void close(Connection connection) {
        if (this.prepareStatement != null) {
            this.flush();

            try {
                this.prepareStatement.close();
            } catch (SQLException var14) {
                log.info("JDBC statement could not be closed: " + var14.getMessage());
            } finally {
                this.prepareStatement = null;
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException var12) {
                log.info("JDBC connection could not be closed: " + var12.getMessage());
            } finally {
                connection = null;
            }
        }
    }

    public void writeToPreparedStatement(Row row) throws IOException {
        if (types != null && types.length > 0 && types.length != row.getArity()) {
            log.warn("Column SQL types array doesn't match arity of passed Row! Check the passed array...");
        }

        try {
            int index;
            if (types == null) {
                for (index = 0; index < row.getArity(); ++index) {
                    log.warn("Unknown column type for column {}. Best effort approach to set its value: {}.", index + 1, row.getField(index));
                    prepareStatement.setObject(index + 1, row.getField(index));
                }
            } else {
                for (index = 0; index < row.getArity(); ++index) {
                    if (row.getField(index) == null) {
                        prepareStatement.setNull(index + 1, types[index]);
                    } else {
                        switch (types[index]) {
                            case -16:
                            case -15:
                            case -1:
                            case 1:
                            case 12:
                                prepareStatement.setString(index + 1, (String) row.getField(index));
                                break;
                            case -7:
                            case 16:
                                prepareStatement.setBoolean(index + 1, (Boolean) row.getField(index));
                                break;
                            case -6:
                                prepareStatement.setByte(index + 1, (Byte) row.getField(index));
                                break;
                            case -5:
                                prepareStatement.setLong(index + 1, (Long) row.getField(index));
                                break;
                            case -4:
                            case -3:
                            case -2:
                                prepareStatement.setBytes(index + 1, (byte[]) ((byte[]) row.getField(index)));
                                break;
                            case 0:
                                prepareStatement.setNull(index + 1, types[index]);
                                break;
                            case 2:
                            case 3:
                                prepareStatement.setBigDecimal(index + 1, (BigDecimal) row.getField(index));
                                break;
                            case 4:
                                prepareStatement.setInt(index + 1, (Integer) row.getField(index));
                                break;
                            case 5:
                                prepareStatement.setShort(index + 1, (Short) row.getField(index));
                                break;
                            case 6:
                            case 8:
                                prepareStatement.setDouble(index + 1, (Double) row.getField(index));
                                break;
                            case 7:
                                prepareStatement.setFloat(index + 1, (Float) row.getField(index));
                                break;
                            case 91:
                                prepareStatement.setDate(index + 1, (Date) row.getField(index));
                                break;
                            case 92:
                                prepareStatement.setTime(index + 1, (Time) row.getField(index));
                                break;
                            case 93:
                                prepareStatement.setTimestamp(index + 1, (Timestamp) row.getField(index));
                                break;
                            default:
                                prepareStatement.setObject(index + 1, row.getField(index));
                                log.warn("Unmanaged sql type ({}) for column {}. Best effort approach to set its value: {}.", new Object[]{types[index], index + 1, row.getField(index)});
                        }
                    }
                }
            }

            prepareStatement.addBatch();
        } catch (SQLException var3) {
            throw new RuntimeException("Preparation of JDBC statement failed.", var3);
        }
    }
}