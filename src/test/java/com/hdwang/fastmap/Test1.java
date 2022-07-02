package com.hdwang.fastmap;

import java.util.Map;
import java.util.SortedMap;

/**
 * 功能测试
 * @author wanghuidong
 * 时间： 2022/6/26 12:59
 */
public class Test1 {

    public static void main(String[] args) {
        boolean enableSort = true; //是否支持排序
        IFastMap<Long, String> fastMap = new FastMap<>(true, enableSort);
        fastMap.put(2L, "aaa2");
        fastMap.put(1L, "aaa1");
        fastMap.put(20L, "aaa20");
        fastMap.put(16L, "aaa16");
        fastMap.put(5L, "aaa5");
        fastMap.put(3L, "aaa3");
        fastMap.put(38L, "aaa38");
        fastMap.put(4L, "aaa4");
        fastMap.put(32L, "aaa32");
        //自然序输出
        System.out.println("FastMap自然序输出");
        for (Map.Entry<Long, String> entry : fastMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        try {
            System.out.println("FastMap输出2-20的值");
            Map<Long, String> sortedMap = fastMap.subMap(2L, 21L);
            for (Map.Entry<Long, String> entry : sortedMap.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        //过期测试
        System.out.println("过期测试");
        Long key = 3L;
        String val = fastMap.get(key);
        System.out.println("key:" + key + "的值：" + val);
        Long timestamp = System.currentTimeMillis();
        System.out.println("当前时间：" + timestamp);
        timestamp = fastMap.expire(key, 10000L);
        System.out.println("过期时间；" + timestamp);

        int i = 0;
        while (true) {
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Long ttl = fastMap.ttl(key);
            System.out.println("key:" + key + "存活时间；" + ttl);
            if (ttl == null) {
                break;
            }
            i++;

            if (i == 3) {
                //续命
                System.out.println("重新设置过期时间");
                fastMap.expire(key, 20000L);
                ttl = fastMap.ttl(key);
                System.out.println("key:" + key + "存活时间；" + ttl);
            }
        }

        System.out.println("FastMap自然序输出");
        for (Map.Entry<Long, String> entry : fastMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }

    }
}
