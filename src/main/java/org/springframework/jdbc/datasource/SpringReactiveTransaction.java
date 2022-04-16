package org.springframework.jdbc.datasource;

import org.apache.ibatis.transaction.Transaction;
import org.mybatis.logging.Logger;
import org.mybatis.logging.LoggerFactory;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.springframework.util.Assert.notNull;

public class SpringReactiveTransaction implements Transaction {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpringReactiveTransaction.class);

  private final DataSource dataSource;

  private Connection connection;

  private boolean isConnectionTransactional;

  private boolean autoCommit;

  public SpringReactiveTransaction(DataSource dataSource) {
    notNull(dataSource, "No DataSource specified");
    this.dataSource = dataSource;
  }

  public SpringReactiveTransaction(Connection connection) {
    notNull(connection, "No Connection specified");
    this.connection = connection;
    this.dataSource = null;
  }

  @Override
  public Connection getConnection() throws SQLException {
    if (this.connection == null) {
      openConnection();
    }
    return this.connection;
  }

  private void openConnection() throws SQLException {
    this.connection = ReactiveDataSourceUtils.getConnection(this.dataSource).block();
    this.autoCommit = this.connection.getAutoCommit();
    this.isConnectionTransactional = ReactiveDataSourceUtils.isConnectionTransactional(this.connection, this.dataSource);
    LOGGER.debug(() -> "JDBC Connection [" + this.connection + "] will"
            + (this.isConnectionTransactional ? " " : " not ") + "be managed by Spring");
  }

  @Override
  public void commit() throws SQLException {
    if (this.connection != null && !this.isConnectionTransactional && !this.autoCommit) {
      LOGGER.debug(() -> "Committing JDBC Connection [" + this.connection + "]");
      this.connection.commit();
    }
  }

  @Override
  public void rollback() throws SQLException {
    if (this.connection != null && !this.isConnectionTransactional && !this.autoCommit) {
      LOGGER.debug(() -> "Rolling back JDBC Connection [" + this.connection + "]");
      this.connection.rollback();
    }
  }

  @Override
  public void close() throws SQLException {
    ReactiveDataSourceUtils.releaseConnection(this.connection, this.dataSource);
  }

  @Override
  public Integer getTimeout() throws SQLException {
    return null;
  }

}
