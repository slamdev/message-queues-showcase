package com.github.slamdev.mq;

public class FileQueueServiceTest extends AbstractQueueServiceTest {

    @Override
    public QueueService createQueueService() {
        return new FileQueueService();
    }
}
