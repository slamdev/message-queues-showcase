package com.github.slamdev.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class InMemoryQueueService implements QueueService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileQueueService.class);

    private final Map<String, List<MessageDescriptor>> queueMessages = new ConcurrentHashMap<>();

    @Override
    public void delete(String queueUrl, String receiptHandle) {
        queueMessages.get(queueUrl).removeIf(m -> m.message.getReceiptHandle().equals(receiptHandle));
    }

    @Override
    public void push(String queueUrl, String messageBody) {
        Message message = new Message(messageBody, UUID.randomUUID().toString());
        MessageDescriptor descriptor = new MessageDescriptor(message);
        queueMessages.computeIfAbsent(queueUrl, k -> new CopyOnWriteArrayList<>()).add(descriptor);
    }

    /**
     * For thread level lock there are two options:
     * - synchronization on method level -> low throughput since all threads will wait until one will finish it's job
     * - lock per object -> other threads will skip locked object and process other objects
     * <p>
     * Current implementation uses lock per file and lock per object options.
     */
    @Override
    public Message pull(String queueUrl, int visibilityTimeout) {
        List<MessageDescriptor> messages = queueMessages.get(queueUrl);
        for (MessageDescriptor descriptor : messages) {
            if (descriptor.expireTime.isBefore(Instant.now()) && descriptor.lock.tryLock()) {
                try {
                    descriptor.expireTime = Instant.now().plus(Duration.ofSeconds(visibilityTimeout));
                    LOGGER.info("Message found: {}", descriptor.message);
                    return descriptor.message;
                } finally {
                    descriptor.lock.unlock();
                }
            }
        }
        return null;
    }

    private static class MessageDescriptor {
        final Message message;
        final Lock lock;
        Instant expireTime;

        private MessageDescriptor(Message message) {
            this.message = message;
            this.lock = new ReentrantLock();
            this.expireTime = Instant.MIN;
        }
    }
}
