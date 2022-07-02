package com.hdwang.fastmap;

import java.util.Date;

/**
 * 过期回调功能测试
 *
 * @author wanghuidong
 * 时间： 2022/7/2 14:43
 */
public class Test5 {

    public static void main(String[] args) {
        IFastMap<Long, String> fastMap = new FastMap<>(true);
        Long key = 1L;
        String val = "aaa1";
        fastMap.put(key, val);
        System.out.println(String.format("time:%s,put:key=%s,val=%s", new Date(), key, val));
        Long expireTime = fastMap.expire(key, 5000L, new ExpireCallback() {
            @Override
            public void onExpire(Object key, Object val) {
                System.out.println(String.format("time:%s,ExpireCallback:key=%s,val=%s,curTime=%s", new Date(), key, val, System.currentTimeMillis()));
            }
        });
        System.out.println(String.format("time:%s,expire:key=%s,expireTime=%s", new Date(), key, expireTime));

        key = 2L;
        val = "aaa2";
        fastMap.put(key, val);
        System.out.println(String.format("time:%s,put:key=%s,val=%s", new Date(), key, val));
        expireTime = fastMap.expire(key, 10000L, new ExpireCallback() {
            @Override
            public void onExpire(Object key, Object val) {
                System.out.println(String.format("time:%s,ExpireCallback:key=%s,val=%s,curTime=%s", new Date(), key, val, System.currentTimeMillis()));
            }
        });
        System.out.println(String.format("time:%s,expire:key=%s,expireTime=%s", new Date(), key, expireTime));
        expireTime = fastMap.expire(key,20500L);
        System.out.println(String.format("time:%s,expire:key=%s,expireTime=%s", new Date(), key, expireTime));

        try {
            //等待1分钟，主线程才退出
            Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
