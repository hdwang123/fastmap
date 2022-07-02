package com.hdwang.fastmap.example;

import com.hdwang.fastmap.ExpireCallback;
import com.hdwang.fastmap.FastMap;
import com.hdwang.fastmap.IFastMap;

import java.util.Date;

/**
 * 案例
 *
 * @author wanghuidong
 * 时间： 2022/7/2 22:00
 */
public class Main {

    public static void main(String[] args) throws Exception {
        //construct a FastMap
        IFastMap<Long, String> fastMap = new FastMap<>();
        Long key = 1L;
        String val = "张三";

        //add key-val to map
        fastMap.put(key, val);
        System.out.printf("time:%s,put:key=%s,val=%s\n", new Date(), key, val);

        //get val by key
        String getVal = fastMap.get(key);
        System.out.printf("time:%s,get:key=%s,val=%s\n", new Date(), key, getVal);

        //set key expire time in milliseconds
        Long expireTime = fastMap.expire(key, 1000L);
        System.out.printf("time:%s,expire:key=%s,expireTime=%s\n", new Date(), key, expireTime);

        //get key time to live
        Long ttl = fastMap.ttl(key);
        System.out.printf("time:%s,ttl:key=%s,ttl=%sms\n", new Date(), key, ttl);

        //set expire callback method
        expireTime = fastMap.expire(key, 2000L, new ExpireCallback<Long, String>() {
            @Override
            public void onExpire(Long key, String val) {
                System.out.printf("time:%s,ExpireCallback:key=%s is expired,val=%s,nowTime=%s\n", new Date(), key, val, System.currentTimeMillis());
            }
        });
        System.out.printf("time:%s,expire:key=%s,expireTime=%s\n", new Date(), key, expireTime);


        //construct a sortable FastMap
        fastMap = new FastMap<>(false, true);
        fastMap.put(3L, "小明");
        fastMap.put(1L, "张三");
        fastMap.put(2L, "李四");

        //print sorted entrySet
        System.out.println(fastMap.entrySet());

        //get range data [1,2] by subMap method
        System.out.println(fastMap.subMap(1L, 3L));

        //main thread wait
        Thread.sleep(60 * 1000);
    }
}
