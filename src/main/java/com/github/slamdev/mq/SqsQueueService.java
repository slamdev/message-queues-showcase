package com.github.slamdev.mq;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import java.util.List;

public class SqsQueueService implements QueueService {

    private final AmazonSQSClient sqsClient;

    public SqsQueueService(AmazonSQSClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    @Override
    public void delete(String queueUrl, String receiptHandle) {
        sqsClient.deleteMessage(queueUrl, receiptHandle);
    }

    @Override
    public Message pull(String queueUrl, int visibilityTimeout) {
        ReceiveMessageRequest request = new ReceiveMessageRequest();
        request.setQueueUrl(queueUrl);
        request.setVisibilityTimeout(visibilityTimeout);
        ReceiveMessageResult response = sqsClient.receiveMessage(request);
        List<com.amazonaws.services.sqs.model.Message> messages = response.getMessages();
        if (messages.isEmpty()) {
            return null;
        }
        com.amazonaws.services.sqs.model.Message message = messages.get(0);
        return new Message(message.getBody(), message.getReceiptHandle());
    }

    @Override
    public void push(String queueUrl, String messageBody) {
        sqsClient.sendMessage(queueUrl, messageBody);
    }
}
