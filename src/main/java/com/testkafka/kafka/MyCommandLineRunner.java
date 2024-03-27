package com.testkafka.kafka;

import com.testkafka.kafka.producer.TopicProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class MyCommandLineRunner implements CommandLineRunner {

    @Autowired
    private TopicProducer topicProducer;

    @Override
    public void run(String... args) throws Exception {
        topicProducer.send();
    }
}

