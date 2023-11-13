package com.uet.microservices.services.worker;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class Utils {
    public static Executor executor = Executors.newFixedThreadPool(10);

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
