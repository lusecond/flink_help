package com.feiyu.thread_switching;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Future
 * Futures是一个抽象的概念，它表示一个值，该值可能在某一点变得可用。一个Future要么获得
 计算完的结果，要么获得计算失败后的异常。Java在java.util.concurrent包中附带了Future接口，
 它使用Executor异步执行。例
 如下面的代码，每传递一个Runnable对象到ExecutorService.submit()方法就会得到一个回调的Future，
 你能使用它检测是否执行，这种方法可以是同步等待线处理结果
 完成。
 */
public class NettyTest {

    public static void main(String[] args) {
        System.out.println("main线程ID：" + Thread.currentThread().getId());

        //实现一个Callable接口
        Callable<Netty> c = new Callable<NettyTest.Netty>() {

            @Override
            public Netty call() throws Exception {
                //这里是你的业务逻辑处理
                System.out.println("线程ID：" + Thread.currentThread().getId());

                //让当前线程阻塞1秒看下效果
                Thread.sleep(1000);
                return new Netty("张三");
            }
        };

        ExecutorService es = Executors.newFixedThreadPool(2);

        //记得要用submit，执行Callable对象
        Future<Netty> fn = es.submit(c);
        Future<Netty> fn2 = es.submit(c);
        //一定要调用这个方法，不然executorService.isTerminated()永远不为true
        es.shutdown();

        //无限循环等待任务处理完毕  如果已经处理完毕 isDone返回true
        while (!fn.isDone()) {
            try {
                //处理完毕后返回的结果
                Netty nt = fn.get();
                System.out.println(nt.name);
                Netty nt2 = fn2.get();
                System.out.println(nt2.name);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }







    //POJO类
    static class Netty {
        private String name;

        private Netty(String name) {
            this.name = name;
        }
    }

}