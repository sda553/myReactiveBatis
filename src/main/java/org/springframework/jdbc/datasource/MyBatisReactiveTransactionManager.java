package org.springframework.jdbc.datasource;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.Nullable;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.reactive.AbstractReactiveTransactionManager;
import org.springframework.transaction.reactive.GenericReactiveTransaction;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionSynchronizationUtils;
import org.springframework.util.Assert;
import reactor.core.publisher.Mono;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class MyBatisReactiveTransactionManager extends AbstractReactiveTransactionManager  implements InitializingBean  {

    public void setDataSource(@Nullable DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Nullable
    public DataSource getDataSource() {
        return dataSource;
    }

    protected DataSource obtainDataSource() {
        DataSource dataSource = getDataSource();
        Assert.state(dataSource != null, "No DataSource set");
        return dataSource;
    }

    @Nullable
    private DataSource dataSource;

    public int getDefaultTimeout() {
        return defaultTimeout;
    }

    public void setDefaultTimeout(int defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
    }

    private int defaultTimeout = TransactionDefinition.TIMEOUT_DEFAULT;

    public boolean isNestedTransactionAllowed() {
        return nestedTransactionAllowed;
    }

    public void setNestedTransactionAllowed(boolean nestedTransactionAllowed) {
        this.nestedTransactionAllowed = nestedTransactionAllowed;
    }

    private boolean nestedTransactionAllowed = false;

    public boolean isEnforceReadOnly() {
        return enforceReadOnly;
    }

    public void setEnforceReadOnly(boolean enforceReadOnly) {
        this.enforceReadOnly = enforceReadOnly;
    }

    private boolean enforceReadOnly = false;

    public MyBatisReactiveTransactionManager() {
    }

    protected int determineTimeout(TransactionDefinition definition) {
        if (definition.getTimeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
            return definition.getTimeout();
        }
        return getDefaultTimeout();
    }

    protected RuntimeException translateException(String task, SQLException ex) {
        return new TransactionSystemException(task + " failed", ex);
    }

    @Override
    protected Object doGetTransaction(TransactionSynchronizationManager synchronizationManager) throws TransactionException {
        DataSourceTransactionObject txObject = new DataSourceTransactionObject();
        txObject.setSavepointAllowed(isNestedTransactionAllowed());
        ConnectionHolder conHolder = (ConnectionHolder) synchronizationManager.getResource(obtainDataSource());
        txObject.setConnectionHolder(conHolder, false);
        return txObject;
    }

    @Override
    protected Mono<Void> doBegin(TransactionSynchronizationManager synchronizationManager, Object transaction, TransactionDefinition definition) throws TransactionException {
        DataSourceTransactionObject txObject = (DataSourceTransactionObject) transaction;
        Mono<Connection> connectionMono = Mono.fromCallable(() -> {
            if (!txObject.hasConnectionHolder() || txObject.getConnectionHolder().isSynchronizedWithTransaction()) {
                Connection newCon = DataSourceUtils.getConnection(this.dataSource);
                if (logger.isDebugEnabled()) {
                    logger.debug("Acquired Connection [" + newCon + "] for JDBC transaction");
                }
                txObject.setConnectionHolder(new ConnectionHolder(newCon), true);
                return newCon;
            } else {
                txObject.getConnectionHolder().setSynchronizedWithTransaction(true);
                return txObject.getConnectionHolder().getConnection();
            }
        });
        return connectionMono.flatMap(con-> prepareTransactionalConnection(con, definition, transaction)
                .doOnSuccess(v -> {
                    txObject.getConnectionHolder().setTransactionActive(true);
                    int timeout = determineTimeout(definition);
                    if (timeout != TransactionDefinition.TIMEOUT_DEFAULT) {
                        txObject.getConnectionHolder().setTimeoutInSeconds(timeout);
                    }
                    if (txObject.isNewConnectionHolder()) {
                        synchronizationManager.bindResource(obtainDataSource(), txObject.getConnectionHolder());
                    }
                }).thenReturn(con).onErrorResume(e -> {
                    if (txObject.isNewConnectionHolder()) {
                        DataSourceUtils.releaseConnection(con, obtainDataSource());
                        txObject.setConnectionHolder(null, false);
                    }
                    return Mono.error(e);
                })).onErrorResume(e -> {
            CannotCreateTransactionException ex = new CannotCreateTransactionException("Could not open JDBC Connection for transaction", e);
            return Mono.error(ex);
        }).then();
    }

    @Override
    protected Mono<Void> doCommit(TransactionSynchronizationManager synchronizationManager, GenericReactiveTransaction status) throws TransactionException {
        DataSourceTransactionObject txObject = (DataSourceTransactionObject) status.getTransaction();
        Connection con = txObject.getConnectionHolder().getConnection();
        if (status.isDebug()) {
            logger.debug("Committing JDBC transaction on Connection [" + con + "]");
        }
        return Mono.fromCallable(()->{
            con.commit();
            return Mono.empty();
        }).onErrorMap(SQLException.class, ex -> translateException("JDBC commit", ex)).then();
    }

    @Override
    protected Mono<Void> doRollback(TransactionSynchronizationManager synchronizationManager, GenericReactiveTransaction status) throws TransactionException {
        DataSourceTransactionObject txObject = (DataSourceTransactionObject) status.getTransaction();
        Connection con = txObject.getConnectionHolder().getConnection();
        if (status.isDebug()) {
            logger.debug("Rolling back JDBC transaction on Connection [" + con + "]");
        }
        return Mono.fromCallable(()->{
            con.rollback();
            return Mono.empty();
        }).onErrorMap(SQLException.class, ex -> translateException("JDBC rollback", ex)).then();
    }

    public MyBatisReactiveTransactionManager(DataSource dataSource) {
        this();
        setDataSource(dataSource);
        afterPropertiesSet();
    }

    @Override
    public void afterPropertiesSet() {
        if (getDataSource() == null) {
            throw new IllegalArgumentException("Property 'dataSource' is required");
        }
    }

    @Override
    protected Mono<Object> doSuspend(TransactionSynchronizationManager synchronizationManager, Object transaction)
            throws TransactionException {

        return Mono.defer(() -> {
            DataSourceTransactionObject txObject = (DataSourceTransactionObject) transaction;
            txObject.setConnectionHolder(null);
            return Mono.justOrEmpty(synchronizationManager.unbindResource(obtainDataSource()));
        });
    }

    @Override
    protected Mono<Void> doResume(TransactionSynchronizationManager synchronizationManager,
                                  @Nullable Object transaction, Object suspendedResources) throws TransactionException {

        return Mono.fromRunnable(() -> synchronizationManager.bindResource(obtainDataSource(), suspendedResources));
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) {
        DataSourceTransactionObject txObject = (DataSourceTransactionObject) transaction;
        return (txObject.hasConnectionHolder() && txObject.getConnectionHolder().isTransactionActive());
    }

    @Override
    protected Mono<Void> doSetRollbackOnly(TransactionSynchronizationManager synchronizationManager,
                                           GenericReactiveTransaction status) throws TransactionException {

        return Mono.fromRunnable(() -> {
            DataSourceTransactionObject txObject = (DataSourceTransactionObject) status.getTransaction();
            if (status.isDebug()) {
                logger.debug("Setting JDBC transaction [" + txObject.getConnectionHolder().getConnection() +
                        "] rollback-only");
            }
            txObject.setRollbackOnly();
        });
    }

    @Override
    protected Mono<Void> doCleanupAfterCompletion(TransactionSynchronizationManager synchronizationManager,
                                                  Object transaction) {

        return Mono.defer(() -> {
            DataSourceTransactionObject txObject = (DataSourceTransactionObject) transaction;

            // Remove the connection holder from the context, if exposed.
            if (txObject.isNewConnectionHolder()) {
                synchronizationManager.unbindResource(obtainDataSource());
            }

            // Reset connection.
            Connection con = txObject.getConnectionHolder().getConnection();

            Mono<Void> afterCleanup = Mono.empty();

            if (txObject.isMustRestoreAutoCommit()) {
                afterCleanup = afterCleanup.then(Mono.fromCallable(()->{con.setAutoCommit(true); return Mono.empty();})).then();
            }
            afterCleanup = afterCleanup
                    .then(Mono.fromRunnable(()-> DataSourceUtils.resetConnectionAfterTransaction(
                            con, txObject.getPreviousIsolationLevel(), txObject.isReadOnly())));

            return afterCleanup.then(Mono.defer(() -> {
                try {
                    if (txObject.isNewConnectionHolder()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Releasing R2DBC Connection [" + con + "] after transaction");
                        }
                        DataSourceUtils.releaseConnection(con, this.dataSource);
                    }
                }
                finally {
                    txObject.getConnectionHolder().clear();
                }
                return Mono.empty();
            }));
        });
    }

    protected Mono<Void> prepareTransactionalConnection(
            Connection con, TransactionDefinition definition, Object transaction) {
        return Mono.fromCallable(()-> {
            DataSourceTransactionObject txObject = (DataSourceTransactionObject) transaction;
            if (isEnforceReadOnly() && definition.isReadOnly()) {
                try (Statement stmt = con.createStatement()) {
                    stmt.executeUpdate("SET TRANSACTION READ ONLY");
                }
            }
            // Apply specific isolation level, if any.
            Integer previousIsolationLevel = DataSourceUtils.prepareConnectionForTransaction(con, definition);
            txObject.setPreviousIsolationLevel(previousIsolationLevel);
            txObject.setReadOnly(definition.isReadOnly());

            if (con.getAutoCommit()) {
                txObject.setMustRestoreAutoCommit(true);
                if (logger.isDebugEnabled()) {
                    logger.debug("Switching JDBC Connection [" + con + "] to manual commit");
                }
                con.setAutoCommit(false);
            }
            return Mono.empty();
        }).then();
    }

    private static class DataSourceTransactionObject extends JdbcTransactionObjectSupport {

        private boolean newConnectionHolder;

        private boolean mustRestoreAutoCommit;

        public void setConnectionHolder(@Nullable ConnectionHolder connectionHolder, boolean newConnectionHolder) {
            super.setConnectionHolder(connectionHolder);
            this.newConnectionHolder = newConnectionHolder;
        }

        public boolean isNewConnectionHolder() {
            return this.newConnectionHolder;
        }

        public void setMustRestoreAutoCommit(boolean mustRestoreAutoCommit) {
            this.mustRestoreAutoCommit = mustRestoreAutoCommit;
        }

        public boolean isMustRestoreAutoCommit() {
            return this.mustRestoreAutoCommit;
        }

        public void setRollbackOnly() {
            getConnectionHolder().setRollbackOnly();
        }

        @Override
        public boolean isRollbackOnly() {
            return getConnectionHolder().isRollbackOnly();
        }

        @Override
        public void flush() {
            if (org.springframework.transaction.support.TransactionSynchronizationManager.isSynchronizationActive()) {
                TransactionSynchronizationUtils.triggerFlush();
            }
        }
    }

}
