package com.hdwang.fastmap;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 并发测试
 *
 * @author wanghuidong
 * 时间： 2022/6/27 9:58
 */
public class ConcurrencyTest {

    public static void main(String[] args) throws Exception {
        IFastMap<Long, String> map = new FastMap<>(true, true);
        AtomicInteger atomicInteger = new AtomicInteger(0);
//        map.put(20L, "aaa20");
//        map.put(32L, "aaa32");
//        map.put(48L, "aaa48");
//        map.put(60L, "aaa60");
//        map.put(55L, "aaa55");
//        map.put(120L, "aaa120");
//        map.put(128L, "aaa128");

        //多线程并发写入测试
        int writeThreadSize = 4;
        int writeKeySize = 10;
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < writeThreadSize; i++) {
            String threadName = "writeThread" + i;
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    //每个线程写入10个key
                    for (int i = 0; i < writeKeySize; i++) {
                        int number = atomicInteger.getAndIncrement();
                        Long key = (long) number;
                        String val = "aaa" + number;
                        map.put(key, val);
                        System.out.println(String.format("thread:%s,put：key=%s,val=%s", Thread.currentThread().getName(), key, val));

                        long ms = 10000L;
                        map.expire(key, ms);
                        System.out.println(String.format("thread:%s,expire：key=%s,ms=%s", Thread.currentThread().getName(), key, ms));

                        try {
                            Thread.sleep(1000L);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                    }
                }
            }, threadName);
            thread.start();
            threads.add(thread);
        }
//        //等待写入完毕
//        for (Thread thread : threads) {
//            thread.join();
//        }
//        //校验写入是否有问题
//        for (int i = 0; i < writeThreadSize * writeKeySize; i++) {
//            Long key = (long) i;
//            if (!map.get(key).equals("aaa" + i)) {
//                System.out.println("并发写入数据校验失败");
//                break;
//            }
//        }
//        System.out.println("并发写入数据校验成功");

        //多线程读测试
        int readThreadSize = 4;
        for (int i = 0; i < readThreadSize; i++) {
            String threadName = "readThread" + i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        Set<Long> keys = map.keySet();
                        System.out.println(String.format("thread:%s,keySet:%s", Thread.currentThread().getName(), keys));
                        if (keys.isEmpty()) {
                            break;
                        }

//                        Collection<String> values = map.values();
//                        System.out.println(String.format("thread:%s,values:%s", Thread.currentThread().getName(), values));
//
//                        Set<Map.Entry<Long, String>> entrySet = map.entrySet();
//                        System.out.println(String.format("thread:%s,entrySet:%s", Thread.currentThread().getName(), entrySet));

//                        Map<Long, String> subMap = map.subMap(map.firstKey(), map.lastKey());
//                        System.out.println(String.format("thread:%s,subMap:%s", Thread.currentThread().getName(), subMap));


//                        for (Map.Entry<Long, String> entry : entrySet) {
//                            System.out.println(String.format("entry key=%s,val=%s", entry.getKey(), entry.getValue()));
//                        }

                        int keyIndex = 0;
                        for (Long key : keys) {
                            String val = map.get(key);
                            System.out.println(String.format("thread:%s,get:key=%s,val=%s", Thread.currentThread().getName(), key, val));

                            Long ttl = map.ttl(key);
                            System.out.println(String.format("thread:%s,ttl:key=%s,tt=%s", Thread.currentThread().getName(), key, ttl));

                            keyIndex++;
                            if (keyIndex == 1) {
                                break;
                            }
                        }


                        try {
                            Thread.sleep(1000L);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }, threadName).start();
        }
    }
}
