package com.github.slamdev.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.WRITE;

public class FileQueueService implements QueueService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileQueueService.class);

    private static final String FILE_NAME = "%d.%s.txt";

    private final Map<String, Lock> messageLocks = new ConcurrentHashMap<>();

    private final Path storage;

    public FileQueueService() {
        storage = propagate(() -> Files.createTempDirectory("queues"));
    }

    @Override
    public void delete(String queueUrl, String receiptHandle) {
        propagate(() -> Files.deleteIfExists(storage.resolve(queueUrl).resolve(receiptHandle)));
        messageLocks.remove(receiptHandle);
    }

    @Override
    public void push(String queueUrl, String messageBody) {
        Path queueDir = storage.resolve(queueUrl);
        propagate(() -> Files.createDirectories(queueDir));
        Message message = new Message(messageBody, UUID.randomUUID().toString());
        String fileName = String.format(FILE_NAME, 0, message.getReceiptHandle());
        Path messageFile = propagate(() -> Files.createFile(queueDir.resolve(fileName)));
        messageLocks.put(message.getReceiptHandle(), new ReentrantLock());
        propagate(() -> Files.write(messageFile, message.getMessageBody().getBytes(UTF_8)));
    }

    /**
     * Lock should be performed on both file system and thread levels,
     * since Java doesn't guarantee that file system lock will be taken into account in multi-thread processing
     * <p>
     * For file system lock there are two options:
     * - shared file lock -> low throughput since all processes will wait until one will finish it's job
     * - lock per file -> other processes will skip locked file and process other files
     * <p>
     * For thread level lock there are two options:
     * - synchronization on method level -> low throughput since all threads will wait until one will finish it's job
     * - lock per object -> other threads will skip locked object and process other objects
     * <p>
     * Current implementation uses lock per file and lock per object options.
     */
    @Override
    public Message pull(String queueUrl, int visibilityTimeout) {
        Path queueDir = storage.resolve(queueUrl);
        Stream<Path> stream = propagate(() -> Files.list(queueDir));
        List<Path> files = stream.collect(Collectors.toList());
        // Stream from Files.list should be closed explicitly,
        // see https://stackoverflow.com/questions/26334421/java-nio-file-filesystemexception-proc-too-many-open-files
        stream.close();
        for (Path file : files) {
            if (extractExpireTime(file).isAfter(Instant.now())) {
                continue;
            }
            Lock lock = messageLocks.get(extractReceiptHandle(file));
            Message message = withThreadLock(lock, () -> withFileLock(file, () -> {
                Path newFile = assignExpireTime(file, visibilityTimeout);
                return convertToMessage(newFile);
            }));
            if (message != null) {
                LOGGER.info("Message found: {}", message.getMessageBody());
                return message;
            }
        }
        return null;
    }

    private <T> T withThreadLock(Lock lock, Supplier<T> action) {
        if (lock != null && lock.tryLock()) {
            try {
                return action.get();
            } finally {
                lock.unlock();
            }
        }
        return null;
    }

    private <T> T withFileLock(Path file, Supplier<T> action) {
        if (!Files.exists(file)) {
            // file can not exists if other thread\process renamed it before the lock
            return null;
        }
        FileChannel channel = null;
        FileLock fileLock = null;
        try {
            // try-with-resources should not be used
            // since file lock should be released before closing channel
            channel = FileChannel.open(file, WRITE);
            fileLock = channel.tryLock();
            if (fileLock == null) {
                // skipping if other process hold the file lock already
                return null;
            }
            return action.get();
        } catch (IOException e) {
            LOGGER.warn("", e);
        } finally {
            if (fileLock != null) {
                try {
                    fileLock.release();
                } catch (IOException e) {
                    LOGGER.warn("", e);
                }
            }
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    LOGGER.warn("", e);
                }
            }
        }
        return null;
    }

    private Message convertToMessage(Path file) {
        byte[] messageBody = propagate(() -> Files.readAllBytes(file));
        String fileName = file.getFileName().toString();
        return new Message(new String(messageBody, UTF_8), fileName.split("\\.")[1]);
    }

    private Instant extractExpireTime(Path file) {
        String fileName = file.getFileName().toString();
        long milli = Long.parseLong(fileName.split("\\.")[0]);
        return Instant.ofEpochMilli(milli);
    }

    private String extractReceiptHandle(Path file) {
        String fileName = file.getFileName().toString();
        return fileName.split("\\.")[1];
    }

    private Path assignExpireTime(Path file, int visibilityTimeout) {
        String oldFileName = file.getFileName().toString();
        long milli = Instant.now().plus(Duration.ofSeconds(visibilityTimeout)).toEpochMilli();
        String newFileName = String.format(FILE_NAME, milli, oldFileName.split("\\.")[1]);
        return propagate(() -> Files.move(file, file.getParent().resolve(newFileName)));
    }

    private static <T> T propagate(Callable<T> action) {
        try {
            return action.call();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
