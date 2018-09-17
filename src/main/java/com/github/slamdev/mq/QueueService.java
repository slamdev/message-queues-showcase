package com.github.slamdev.mq;

public interface QueueService {

    void delete(String queueUrl, String receiptHandle);

    Message pull(String queueUrl, int visibilityTimeout);

    void push(String queueUrl, String messageBody);
}
