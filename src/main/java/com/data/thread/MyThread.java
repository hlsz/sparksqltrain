package com.data.thread;

import java.lang.reflect.Executable;

public class MyThread extends Thread {

    @Override
    public void run() {
        super.run();

        try{
         while(true){
             if(this.isInterrupted()){
                 System.out.println("线程被停止了");
                 return;
             }
             System.out.println("TIme:" + System.currentTimeMillis());
         }
        }catch(Exception e) {
           e.printStackTrace();
        }

//        try{
//            System.out.println("线程开始...");
//            Thread.sleep(2000);
//            for (int i = 0; i < 50000; i++) {
//                if(this.isInterrupted()) {
//                    System.out.println("线程已经终止， for循环不再执行");
//                   throw new InterruptedException();
//                }
//                System.out.println("i="+(i+1));
//            }
//            System.out.println("这里for循环外面的语句，也会被执行");
//
//            System.out.println("线程结束");
//        }catch (InterruptedException e) {
//            System.out.println("在沉睡中被停止，进入catch。调用isInterrupted()方法的结果是："+this.isInterrupted());
//            System.out.println("进入mythread类中的Catch了。。。。");
//            e.printStackTrace();
//        }

    }
}

class Run{

    public static void main(String[] args) {
        Thread thread = new MyThread();
        thread.start();
        try{
            Thread.sleep(2000);
            thread.interrupt();

            System.out.println("stop 1-> " + Thread.interrupted());
            System.out.println("stop 2-> " + thread.isInterrupted());
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class Run2 {
    public static void main(String[] args) {
        Thread.currentThread().interrupt();
        System.out.println("stop 1->" + Thread.interrupted());
        System.out.println("stop 2->" + Thread.interrupted());

        System.out.println("end");
    }
}

class  Run3 {
    public static void main(String[] args) {
        Thread thread = new MyThread();
        thread.start();

        thread.interrupt();
        System.out.println("stop 1->"+thread.isInterrupted());
        System.out.println("stop 2->"+thread.isInterrupted());
    }
}

class MyRunnable implements  Runnable {
    @Override
    public void run() {
        System.out.println("MyRunnable");
    }
}
