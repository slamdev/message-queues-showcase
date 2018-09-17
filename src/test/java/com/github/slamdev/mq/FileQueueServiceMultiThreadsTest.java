package com.github.slamdev.mq;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class FileQueueServiceMultiThreadsTest {

    private FileQueueService service;

    private static final Logger LOGGER = LoggerFactory.getLogger(FileQueueServiceMultiThreadsTest.class);

    @Before
    public void setUp() {
        service = new FileQueueService();
    }

    /**
     * This test spawns 100 threads and execute push\pull\delete operations on the queue
     * It can be used only for local development and nether enabled
     */
    @Ignore
    @Test
    public void shouldHandleMultiThreadsOperations() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(100);
        List<Callable<Void>> tasks = IntStream.rangeClosed(0, 1000)
                .mapToObj(this::createExecution)
                .collect(toList());
        executor.invokeAll(tasks);
    }

    private Callable<Void> createExecution(int i) {
        return () -> {
            try {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                service.push("file", "message: " + i);
                TimeUnit.SECONDS.sleep(random.nextInt(1, 5));
                Message message = service.pull("file", random.nextInt(1, 10));
                TimeUnit.SECONDS.sleep(random.nextInt(1, 5));
                service.delete("file", message.getReceiptHandle());
            } catch (Exception e) {
                LOGGER.error("", e);
            }
            return null;
        };
    }
}
