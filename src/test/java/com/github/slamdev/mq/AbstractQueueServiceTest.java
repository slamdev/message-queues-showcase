package com.github.slamdev.mq;

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.function.Supplier;

import static java.time.Duration.ofSeconds;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertThat;

public abstract class AbstractQueueServiceTest {

    private QueueService service;

    abstract QueueService createQueueService();

    @Before
    public void setUp() {
        service = createQueueService();
    }

    @Test
    public void shouldDeleteMessageFromQueue() {
        service.push("queue", "message");
        Message message = service.pull("queue", 0);
        service.delete("queue", message.getReceiptHandle());
        assertThat(service.pull("queue", 0), nullValue());
    }

    @Test
    public void shouldPushMessageToQueue() {
        service.push("queue", "message");
        Message message = service.pull("queue", 0);
        assertThat(message.getMessageBody(), equalTo("message"));
    }

    @Test
    public void shouldNotPullMessageWhenItIsNotVisible() {
        service.push("queue", "message");
        service.pull("queue", 10);
        assertThat(service.pull("queue", 0), nullValue());
    }

    @Test
    public void shouldPullMessageWhenItBecameVisible() {
        service.push("queue", "message");
        service.pull("queue", 1);
        Instant start = Instant.now();
        Message message = executeUntilNonNull(() -> service.pull("queue", 0), ofSeconds(2));
        Duration executionTime = Duration.between(start, Instant.now());
        assertThat(message.getMessageBody(), equalTo("message"));
        // assert that message appeared close to 1 second
        assertThat((double) executionTime.toMillis(), closeTo(ofSeconds(1).toMillis(), 100));
    }

    /**
     * Execute action in separate thread until execution result is not null.
     * Execution will be canceled after specified timeout
     */
    private <T> T executeUntilNonNull(Supplier<T> action, Duration timeout) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<T> future = executor.submit(() -> {
            T result;
            while ((result = action.get()) == null) {
                // checking the action for non-null value until
                // executor kills thread by timeout
            }
            return result;
        });
        executor.shutdown();
        try {
            return future.get(timeout.getSeconds(), TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            return null;
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }
}
