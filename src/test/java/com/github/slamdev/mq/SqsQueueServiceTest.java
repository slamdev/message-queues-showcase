package com.github.slamdev.mq;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SqsQueueServiceTest {

    @InjectMocks
    private SqsQueueService service;

    @Mock
    private AmazonSQSClient sqsClient;

    @Test
    public void shouldCallSqsWhenDeletingMessage() {
        service.delete("queue", "id");
        verify(sqsClient).deleteMessage("queue", "id");
    }

    @Test
    public void shouldCallSqsWhenPushingMessage() {
        service.push("queue", "message");
        verify(sqsClient).sendMessage("queue", "message");
    }

    @Test
    public void shouldCallSqsWhenPullingMessage() {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(mock(ReceiveMessageResult.class));
        service.pull("queue", 10);
        ReceiveMessageRequest request = new ReceiveMessageRequest();
        request.setQueueUrl("queue");
        request.setVisibilityTimeout(10);
        verify(sqsClient).receiveMessage(request);
    }

    @Test
    public void shouldReturnFirstMessageFromSqs() {
        ReceiveMessageResult result = new ReceiveMessageResult();
        com.amazonaws.services.sqs.model.Message sqsMessage = new com.amazonaws.services.sqs.model.Message();
        sqsMessage.setBody("message");
        sqsMessage.setReceiptHandle("id");
        result.setMessages(singletonList(sqsMessage));
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(result);
        Message message = service.pull("queue", 10);
        assertThat(message.getMessageBody(), equalTo("message"));
        assertThat(message.getReceiptHandle(), equalTo("id"));
    }

    @Test
    public void shouldReturnNullWhenNoMessagesInSqs() {
        ReceiveMessageResult result = new ReceiveMessageResult();
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(result);
        Message message = service.pull("queue", 10);
        assertThat(message, nullValue());
    }
}
