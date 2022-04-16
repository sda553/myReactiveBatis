package org.springframework.jdbc.datasource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.lang.Nullable;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.reactive.TransactionContextManager;
import org.springframework.transaction.reactive.TransactionSynchronization;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import reactor.core.publisher.Mono;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class ReactiveDataSourceUtils {

    private static final Log logger = LogFactory.getLog(ReactiveDataSourceUtils.class);


    public static Mono<Connection> getConnection(DataSource dataSource) throws CannotGetJdbcConnectionException {
        return doGetConnection(dataSource).onErrorResume(ex->{
            if (ex instanceof SQLException)
                return Mono.error(new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection", (SQLException)ex));
            if (ex instanceof IllegalStateException)
                return Mono.error(new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection: " + ex.getMessage()));
            return Mono.error(ex);
        });
    }

    public static Mono<Connection> doGetConnection(DataSource dataSource) {
        Assert.notNull(dataSource, "No DataSource specified");
        return TransactionContextManager.currentContext()
                .map(TransactionSynchronizationManager::new)
                .flatMap(sm -> {
                    ConnectionHolder conHolder = (ConnectionHolder) sm.getResource(dataSource);
                    if (conHolder != null && (conHolder.hasConnection() || conHolder.isSynchronizedWithTransaction())) {
                        conHolder.requested();
                        if (!conHolder.hasConnection()) {
                            if (!conHolder.hasConnection()) {
                                logger.debug("Feching resumed JDBC Connection from DataSource");
                                Connection innerCon;
                                try {
                                    innerCon = fetchConnection(dataSource);
                                } catch (SQLException e) {
                                    return Mono.error(e);
                                }
                                conHolder.setConnection(innerCon);
                            }
                        }
                        return Mono.justOrEmpty(conHolder.getConnection());
                    }
                    Connection con;
                    try {
                        con = fetchConnection(dataSource);
                    } catch (SQLException e) {
                        return Mono.error(e);
                    }
                    if (sm.isSynchronizationActive()) {
                        try {
                            // Use same Connection for further JDBC actions within the transaction.
                            // Thread-bound object will get removed by synchronization at transaction completion.
                            ConnectionHolder holderToUse = conHolder;
                            if (holderToUse == null) {
                                holderToUse = new ConnectionHolder(con);
                            } else {
                                holderToUse.setConnection(con);
                            }
                            holderToUse.requested();
                            sm.registerSynchronization(new ConnectionSynchronization(holderToUse, dataSource));
                            holderToUse.setSynchronizedWithTransaction(true);
                            if (holderToUse != conHolder) {
                                sm.bindResource(dataSource, holderToUse);
                            }
                        } catch (RuntimeException ex) {
                            // Unexpected exception from external delegation call -> close Connection and rethrow.
                            releaseConnection(con, dataSource);
                            return Mono.error(ex);
                        }
                    }
                    return Mono.justOrEmpty(con);
                }).onErrorResume(NoTransactionException.class,(e)->{
                    Connection newCon;
                    try {
                        newCon = fetchConnection(dataSource);
                    } catch (SQLException ex) {
                        return Mono.error(ex);
                    }
                    return Mono.justOrEmpty(newCon);
                });
    }

    private static Connection fetchConnection(DataSource dataSource) throws SQLException {
        Connection con = dataSource.getConnection();
        if (con == null) {
            throw new IllegalStateException("DataSource returned null from getConnection(): " + dataSource);
        }
        return con;
    }

    public static boolean isConnectionTransactional(Connection con, @Nullable DataSource dataSource) {
        if (dataSource == null)
            return false;
        return Boolean.TRUE.equals(TransactionContextManager.currentContext()
                .map(TransactionSynchronizationManager::new)
                .map(sm -> {
                    ConnectionHolder conHolder = (ConnectionHolder) sm.getResource(dataSource);
                    return (conHolder != null && connectionEquals(conHolder, con));
                }).onErrorReturn(NoTransactionException.class,Boolean.FALSE).block());
    }

    public static void releaseConnection(@Nullable Connection con, @Nullable DataSource dataSource) {
        doReleaseConnection(con, dataSource).subscribe();
    }

    public static Mono<Void> doReleaseConnection(@Nullable Connection con, @Nullable DataSource dataSource) {
        if (con == null) {
            return Mono.empty();
        }
        Mono<Void> result = Mono.empty();
        if (dataSource != null) {
            result = result.then(TransactionContextManager.currentContext()
                    .map(TransactionSynchronizationManager::new)
                    .flatMap(sm->{
                        ConnectionHolder conHolder = (ConnectionHolder) sm.getResource(dataSource);
                        if (conHolder != null && connectionEquals(conHolder, con)) {
                            // It's the transactional Connection: Don't close it.
                            conHolder.released();
                        }
                        try {
                            doCloseConnection(con, dataSource);
                        } catch (SQLException e) {
                            return Mono.error(e);
                        }
                        return Mono.empty();
                    }));
        }
        result = result.then(Mono.fromCallable(()->{
            doCloseConnection(con, dataSource);
            return Mono.empty();
        }).then());
        return result;
    }

    public static void doCloseConnection(Connection con, @Nullable DataSource dataSource) throws SQLException {
        if (!(dataSource instanceof SmartDataSource) || ((SmartDataSource) dataSource).shouldClose(con)) {
            con.close();
        }
    }

    private static boolean connectionEquals(ConnectionHolder conHolder, Connection passedInCon) {
        if (!conHolder.hasConnection()) {
            return false;
        }
        Connection heldCon = conHolder.getConnection();
        // Explicitly check for identity too: for Connection handles that do not implement
        // "equals" properly, such as the ones Commons DBCP exposes).
        return (heldCon == passedInCon || heldCon.equals(passedInCon) ||
                getTargetConnection(heldCon).equals(passedInCon));
    }

    public static Connection getTargetConnection(Connection con) {
        Connection conToUse = con;
        while (conToUse instanceof ConnectionProxy) {
            conToUse = ((ConnectionProxy) conToUse).getTargetConnection();
        }
        return conToUse;
    }

    private static class ConnectionSynchronization implements TransactionSynchronization {

        private final ConnectionHolder connectionHolder;

        private final DataSource dataSource;

        private boolean holderActive = true;

        public ConnectionSynchronization(ConnectionHolder connectionHolder, DataSource dataSource) {
            this.connectionHolder = connectionHolder;
            this.dataSource = dataSource;
        }

        @Override
        public Mono<Void> suspend() {
            if (this.holderActive) {
                return TransactionContextManager.currentContext()
                        .map(TransactionSynchronizationManager::new)
                        .flatMap(sm->{
                            sm.unbindResource(this.dataSource);
                            if (this.connectionHolder.hasConnection() && !this.connectionHolder.isOpen()) {
                                releaseConnection(this.connectionHolder.getConnection(), this.dataSource);
                                this.connectionHolder.setConnection(null);
                            }
                            return Mono.empty();
                        });
            }
            return Mono.empty();
        }

        @Override
        public Mono<Void> resume() {
            if (this.holderActive) {
                return TransactionContextManager.currentContext()
                        .map(TransactionSynchronizationManager::new)
                        .flatMap(sm->{
                            sm.bindResource(this.dataSource, this.connectionHolder);
                            return Mono.empty();
                        });
            }
            return Mono.empty();
        }

        @Override
        public Mono<Void> beforeCompletion() {
            if (!this.connectionHolder.isOpen()) {
                return TransactionContextManager.currentContext()
                        .map(TransactionSynchronizationManager::new)
                        .flatMap(sm->{
                            sm.unbindResource(this.dataSource);
                            this.holderActive = false;
                            if (this.connectionHolder.hasConnection()) {
                                releaseConnection(this.connectionHolder.getConnection(), this.dataSource);
                            }
                            return Mono.empty();
                        });
            }
            return Mono.empty();
        }

        @Override
        public Mono<Void> afterCompletion(int status) {
            if (this.holderActive) {
                return TransactionContextManager.currentContext()
                        .map(TransactionSynchronizationManager::new)
                        .flatMap(sm->{
                            sm.unbindResourceIfPossible(this.dataSource);
                            this.holderActive = false;
                            if (this.connectionHolder.hasConnection()) {
                                releaseConnection(this.connectionHolder.getConnection(), this.dataSource);
                                // Reset the ConnectionHolder: It might remain bound to the thread.
                                this.connectionHolder.setConnection(null);
                            }
                            this.connectionHolder.reset();
                            return Mono.empty();
                        });
            }
            this.connectionHolder.reset();
            return Mono.empty();

        }
    }
}
