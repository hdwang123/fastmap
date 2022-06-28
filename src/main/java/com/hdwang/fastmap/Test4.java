package com.hdwang.fastmap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 性能测试
 *
 * @author wanghuidong
 * 时间： 2022/6/28 15:02
 */
public class Test4 {

    public static void main(String[] args) {
        //存入100万数据
        IFastMap<Integer, String> fastmap = new FastMap<>(false, true);
        int maxNumber = 100_0000;
        for (int i = 0; i < maxNumber; i++) {
            fastmap.put(i, "a" + i);
        }
        //测试查询性能

        //随机取1万个数
        int getSize = 10000;
        List<Integer> randomInts = new ArrayList<>();
        for (int i = 0; i < getSize; i++) {
            int randomInt = new Random().nextInt(maxNumber);
            randomInts.add(randomInt);
        }

        long startTime = System.currentTimeMillis();
        for (int randomInt : randomInts) {
//            //100万取1万次：6-7ms
            String val = fastmap.get(randomInt);
////            System.out.println("val:" + val);
////            fastmap.entrySet();
        }


//        for (int i = 0; i < getSize; i++) {
            //100万取1万次10条记录：14-15ms
//            Map<Integer, String> map = fastmap.headMap(10);
//            System.out.println(map);

            //100万取1万次10条记录：15-18ms
//            Map<Integer, String> map = fastmap.tailMap(99_9990);
////            System.out.println(map);
//
            //100万取1万次10条记录：14-19ms
//            Map<Integer, String> map = fastmap.subMap(89_9990, 89_9999);
////            System.out.println(map);
//        }
        System.out.println("耗时：" + (System.currentTimeMillis() - startTime) + "ms");

    }
}
