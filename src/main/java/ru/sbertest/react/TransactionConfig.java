package ru.sbertest.react;

import org.apache.ibatis.transaction.TransactionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.MyBatisReactiveTransactionManager;
import org.springframework.transaction.ReactiveTransactionManager;
import ru.sbertest.react.springbatis.SpringReactiveTransactionFactory;

import javax.sql.DataSource;


@Configuration
public class TransactionConfig {
    @Autowired
    DataSource dataSource;

    @Bean
    public ReactiveTransactionManager reactiveTransactionManager() {
        return new MyBatisReactiveTransactionManager(dataSource);
    }

    @Bean
    public TransactionFactory reactiveTransactionFactory() {
        return new SpringReactiveTransactionFactory();
    }
}
