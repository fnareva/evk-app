package com.tasks;


import com.engine.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.concurrent.ExecutionException;

@Component
public class SendMessageTask {
    private final Logger logger = LoggerFactory.getLogger(SendMessageTask.class);
    int i=0;

    private final Producer producer;

    public SendMessageTask(Producer producer) {
        this.producer = producer;
    }

    // run every 3 sec
    @Scheduled(fixedRateString = "3000")
    public void send() throws ExecutionException, InterruptedException {
        try {
            String src = "C:\\Users\\ekollarova\\IdeaProjects\\Local\\src\\main\\resources\\query-response-two-groups-no-tags.json";
            String json = new String(Files.readAllBytes(Paths.get(src)));


        ListenableFuture<SendResult<String, String>> listenableFuture = this.producer.sendMessage("telemetry-v2-mts-general-lax1", "IN_KEY", json);

        logger.info(this.producer.toString());

        SendResult<String, String> result = listenableFuture.get();
        logger.info(String.format("Produced:\ntopic: %s\noffset: %d\npartition: %d\nvalue size: %d", result.getRecordMetadata().topic(),
                result.getRecordMetadata().offset(),
                result.getRecordMetadata().partition(), result.getRecordMetadata().serializedValueSize()));}
        catch (IOException e) {
            System.out.println("no file found");
        }

    }
}