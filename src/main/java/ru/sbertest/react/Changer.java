package ru.sbertest.react;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;
import ru.sbertest.react.entity.State;
import ru.sbertest.react.repositories.StateRepository;

@Service
public class Changer {

    @Autowired
    StateRepository repository;

    public Mono<State> makeSelect() {
        return repository.selectByState(6226);
    }

    @Transactional(rollbackFor = Exception.class)
    public Mono<Void> makeChangeRollbacked() {
        return repository.deleteByState(6226).then(Mono.error(new Exception("Some ex!"))).then();
    }

    @Transactional(rollbackFor = Exception.class)
    public Mono<Void> makeChangeCommited() {
        return repository.deleteByState(6226).then();
    }

}
