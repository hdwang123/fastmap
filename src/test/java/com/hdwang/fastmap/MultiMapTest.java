package com.hdwang.fastmap;

import javax.swing.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 多个FastMap测试
 *
 * @author wanghuidong
 * 时间： 2022/7/4 9:37
 */
public class MultiMapTest {

    public static void main(String[] args) {
//        //并发创建FastMap测试
//        int fastMapSize = 4;
//        List<FastMap> fastMaps = new Vector<>();
//        for (int i = 0; i < 4; i++) {
//            fastMaps.add(new FastMap());
//        }
//        //并发读取FastMap测试
//        int threadSize = 4;
//        //循环并发测试
//        int doTimes = 20;
//        while (true) {
//            for (int i = 0; i < threadSize; i++) {
//                final int index = i;
//                new Thread(new Runnable() {
//                    @Override
//                    public void run() {
//                        for (FastMap map : fastMaps) {
//                            map.put(index, "newVal" + index);
//                            System.out.println(map);
//                        }
//                    }
//                }).start();
//            }
//
//            //延时
//            try {
//                Thread.sleep(1000L);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//            if (doTimes-- == 0) {
//                break;
//            }
//        }


        //多个FastMap过期测试
        IFastMap<Long, IFastMap<String, Object>> fastMap = new FastMap<>();
        Long key1 = 1L;
        IFastMap<String, Object> innerMap = new FastMap<>();
        String a1 = "a1";
        innerMap.put(a1, 1001);
        innerMap.expire(a1, 5000L);

        innerMap.put("a2", 1002);
        innerMap.put("a3", 1003);
        fastMap.put(key1, innerMap);
        fastMap.expire(key1, 10000L);


        Long key2 = 2L;
        innerMap = new FastMap<>();
        String b1 = "b1";
        innerMap.put(b1, 1001);
        innerMap.expire(b1, 12000L);

        innerMap.put("b2", 1002);
        innerMap.expire("b2", 14000L, (key, val) -> {
            System.out.printf("time:%s,thread:%s,expireCallback:key=%s,val=%s\nn",
                    new Date(), Thread.currentThread().getName(), key, val);

            //造成FastMap里的线程耗时，模拟耗时任务
            try {
                Thread.sleep(3000L);
            } catch (InterruptedException ex) {
                System.out.println("InterruptedException");
            }
        });

        innerMap.put("b3", 1003);
        innerMap.expire("b3", 16000L, (key, val) -> {
            System.out.printf("time:%s,thread:%s,expireCallback:key=%s,val=%s\nn",
                    new Date(), Thread.currentThread().getName(), key, val);
        });
        fastMap.put(key2, innerMap);
        fastMap.expire(key2, 20000L);


        while (true) {
            IFastMap<String, Object> innerMap1 = fastMap.get(key1);
//            System.out.println(innerMap1);
            IFastMap<String, Object> innerMap2 = fastMap.get(key2);
//            System.out.println(innerMap2);
            System.out.println("fastMap=" + fastMap);
            if (fastMap.isEmpty()) {
                break;
            }
            System.out.println("===========================================thread sleep ===========================================");
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException ex) {
                System.out.println("InterruptedException");
            }
        }
    }
}
