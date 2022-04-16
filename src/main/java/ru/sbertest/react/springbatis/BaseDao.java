package ru.sbertest.react.springbatis;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.ReactiveDataSourceUtils;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import javax.sql.DataSource;
import java.util.function.Function;

@Component
public abstract class BaseDao {
    @Autowired
    DataSource dataSource;

    private final SqlSessionFactory sqlSessionFactory;

    public BaseDao(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;
    }

    protected <R> Mono<R> apply(Function<SqlSession, R> function) {
        return ReactiveDataSourceUtils.getConnection(dataSource).map(
                connection -> {
                    SqlSession session = sqlSessionFactory.openSession(connection);
                    return function.apply(session);
                });
    }
}
