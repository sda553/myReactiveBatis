package ru.sbertest.react;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;

@Service
public class MainTester {
    @Autowired
    Changer changer;

    @PostConstruct
    void mainTest() throws Exception {
        changer.makeSelect().map(x->{
            return x; //чисто проверить в отладке
        }).subscribe();
        //эта транзакция должна роллбэкнуться
        changer.makeChangeRollbacked().subscribe();
        //а эта транзакция должна выполнится
       // changer.makeChangeCommited().subscribe();

    }
}
