package com.github.slamdev.mq;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.JUnitCore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class FileQueueServiceMultiProcessesTest {

    /**
     * This test spawns 5 separate java processes and runs {@link FileQueueServiceMultiThreadsTest} in all of them
     * It can be used only for local development and nether enabled
     */
    @Ignore
    @Test
    public void shouldHandleMultiProcessesOperations() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        List<Callable<Integer>> tasks = IntStream.range(0, 5)
                .mapToObj(this::createExecution)
                .collect(toList());
        executor.invokeAll(tasks);
    }

    private Callable<Integer> createExecution(int i) {
        return () -> exec(JUnitCore.class, FileQueueServiceMultiThreadsTest.class.getCanonicalName());
    }

    private static int exec(Class type, String... args) {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = type.getCanonicalName();
        List<String> commands = new ArrayList<>();
        commands.add(javaBin);
        commands.add("-cp");
        commands.add(classpath);
        commands.add(className);
        commands.addAll(Arrays.asList(args));
        try {
            Process process = new ProcessBuilder(commands)
                    .inheritIO()
                    .start();
            process.waitFor();
            return process.exitValue();
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }
}
