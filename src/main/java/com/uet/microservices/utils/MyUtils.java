package com.uet.microservices.utils;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class MyUtils {
    public static Executor blockingExecutor = Executors.newFixedThreadPool(10);

    /* Try to find available port on machine */
    public static int findAvailablePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            System.out.println("Port is not available");
            return findAvailablePort();
        }
    }

    /**
     * This function is for simulating a blocking task
     * Calculate sum of numbers from 'from' to 'to'
     */
    public static int sumOf(int from, int to) {
        int sum = 0;
        for (int i = from; i <= to; i++) {
            try {
                Thread.sleep(500);
                sum += i;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return sum;
    }
}
