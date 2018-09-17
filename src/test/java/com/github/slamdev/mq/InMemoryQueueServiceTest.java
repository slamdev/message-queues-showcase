package com.github.slamdev.mq;

public class InMemoryQueueServiceTest extends AbstractQueueServiceTest {

    @Override
    public QueueService createQueueService() {
        return new InMemoryQueueService();
    }
}
