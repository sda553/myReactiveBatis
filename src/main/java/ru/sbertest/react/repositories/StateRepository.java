package ru.sbertest.react.repositories;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import org.apache.ibatis.binding.MapperMethod;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;
import ru.sbertest.react.entity.State;
import ru.sbertest.react.springbatis.BaseDao;

import java.util.Map;

@Repository
public class StateRepository extends BaseDao  {

    private static final String MAPPER_CLASS = "ru.sbertest.react.mappers.StateMapper.";

    public StateRepository(SqlSessionFactory sqlSessionFactory) {
        super(sqlSessionFactory);
    }

    public Mono<Integer> deleteByState(Integer stateFilter) {
        return this.apply((session) -> {
            Map<String, Object> param = new MapperMethod.ParamMap<>();
            QueryWrapper<State> wrapper = Wrappers.<State>query().eq("state", stateFilter);
            param.put("ew",wrapper);
            param.put("param1",wrapper);
            return session.delete(MAPPER_CLASS+"delete",param);
        });
    }

    public Mono<State> selectByState(Integer stateFilter) {
        return this.apply((session) -> {
            Map<String, Object> param = new MapperMethod.ParamMap<>();
            QueryWrapper<State> wrapper = Wrappers.<State>query().eq("state", stateFilter);
            param.put("ew",wrapper);
            param.put("param1",wrapper);
            return session.selectOne(MAPPER_CLASS+"selectList",param);
        });
    }

}