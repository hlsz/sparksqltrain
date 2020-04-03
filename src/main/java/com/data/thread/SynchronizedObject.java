package com.data.thread;

public class SynchronizedObject {
    private String name = "a";
    private String password = "aa";

    public synchronized void printString(String name, String password) {
        try{
         this.name = name;
         Thread.sleep(1000);
         this.password = password;
        }catch(Exception e) {
           e.printStackTrace();
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}

class MyThread2 extends Thread {
    private SynchronizedObject synchronizedObject;
    public MyThread2(SynchronizedObject synchronizedObject) {
        this.synchronizedObject = synchronizedObject;
    }
    @Override
    public void run(){
        synchronizedObject.printString("b","bb");
    }

}

class Run4{
    public static void main(String[] args) throws InterruptedException {
        SynchronizedObject synchronizedObject = new SynchronizedObject();
        Thread thread = new MyThread2(synchronizedObject);

        thread.start();
        Thread.sleep(500);
        thread.stop();
        System.out.println(synchronizedObject.getName()+ " " + synchronizedObject.getPassword());

    }
}
