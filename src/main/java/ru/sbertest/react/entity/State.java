package ru.sbertest.react.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
@TableName("states")
public class State {
    private Long id;
    private Long prevStateId;
    private Long nextStateId;
    private Integer state;
}
