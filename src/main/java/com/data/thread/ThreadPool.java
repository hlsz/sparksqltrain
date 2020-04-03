package com.data.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPool {
    public static int POOL_NUM =10;

    public static void main(String[] args) {

        ExecutorService executorService = Executors.newFixedThreadPool(POOL_NUM);
        for (int i = 0; i < POOL_NUM; i++) {
            RunableThread thread = new RunableThread();
            executorService.execute(thread);
        }
    }
}

class RunableThread implements Runnable {
    private  int THREAD_NUM = 10;

    @Override
    public void run() {
        for (int i = 0; i < THREAD_NUM; i++) {
            System.out.println("线程" + Thread.currentThread() + " " + i);
        }
    }
}
