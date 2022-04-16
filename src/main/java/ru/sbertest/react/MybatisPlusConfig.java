package ru.sbertest.react;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@MapperScan("ru.sbertest.react.mappers")
public class MybatisPlusConfig {
}
