package com.feiyu.test1;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkMysqlProducer<IN> extends TwoPhaseCommitSinkFunction<IN, FlinkMysqlProducer.MysqlTransactionState, FlinkMysqlProducer.MysqlTransactionContext> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkMysqlProducer.class);

    private static final long serialVersionUID = 1L;

    //默认允许的最大转换时间
    public static final Time DEFAULT_MYSQL_TRANSACTION_TIMEOUT = Time.hours(1);

    /**
     * Use default {@link ListStateDescriptor} for internal state serialization. Helpful utilities for using this
     * constructor are {@link TypeInformation#of(Class)}, {@link TypeHint} and
     * {@link TypeInformation#of(TypeHint)}. Example:
     * <pre>
     * {@code
     * TwoPhaseCommitSinkFunction(TypeInformation.of(new TypeHint<State<TXN, CONTEXT>>() {}));
     * }
     * </pre>
     *
     * @param transactionSerializer {@link TypeSerializer} for the transaction type of this sink
     * @param contextSerializer     {@link TypeSerializer} for the context type of this sink
     */
    public FlinkMysqlProducer(TypeSerializer<MysqlTransactionState> transactionSerializer, TypeSerializer<MysqlTransactionContext> contextSerializer) {
        super(transactionSerializer, contextSerializer);
    }

    @Override
    public void open(Configuration configuration) throws Exception {

        super.open(configuration);
    }

    @Override
    protected void invoke(FlinkMysqlProducer.MysqlTransactionState transaction, IN value, Context context) throws Exception {

    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    protected FlinkMysqlProducer.MysqlTransactionState beginTransaction() throws Exception {
        return null;
    }

    @Override
    protected void preCommit(FlinkMysqlProducer.MysqlTransactionState transaction) throws Exception {

    }

    @Override
    protected void commit(FlinkMysqlProducer.MysqlTransactionState transaction) {

    }

    @Override
    protected void abort(FlinkMysqlProducer.MysqlTransactionState transaction) {

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        super.snapshotState(context);
    }

    static class MysqlTransactionState {

    }

    public static class MysqlTransactionContext {

    }
}
