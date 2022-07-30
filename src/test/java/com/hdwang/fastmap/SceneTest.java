package com.hdwang.fastmap;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 、
 * 场景测试
 *
 * @author wanghuidong
 * 时间： 2022/6/26 16:14
 */
public class SceneTest {

    public static void main(String[] args) throws Exception {
        //场景：手机号请求频率限制：1分钟2次，5分钟5次
        IFastMap<String, IFastMap<Long, Long>> fastMap = new FastMap<>(true);


        //指定手机号
        String phone = "13312340001";

        //过期时间设置1分钟
        Long expireTime = fastMap.expire(phone, 1 * 60 * 1000L);
        System.out.println("过期时间；" + expireTime);

        //是否超出限制标识
        AtomicBoolean beyondLimitFlag = new AtomicBoolean(false);

        //=============模拟请求日志插入================
        while (true) {
            //compute原子操作，线程安全，可以用来初始化指定key的值和计算指定key新的值
            fastMap.compute(phone, (k, v) -> {
                //初始化指定key的值
                if (v == null) {
                    //记录每次访问记录：key:时间戳，value:访问次数
                    v = new FastMap<>(false, true);
                }

                //操作指定key的value值
                //1.获取近1分钟的插入次数
                Long nowTime = System.currentTimeMillis();
                Long startTime = nowTime - 1 * 60 * 1000L;
                Map<Long, Long> subMap = v.subMap(startTime, true, nowTime, true);
                System.out.println("phone:" + phone + ",one minute subMap:" + subMap);
                long oneMinuteCount = subMap.size();
                System.out.println("oneMinuteCount:" + oneMinuteCount);

                //2.未超出限制，方可以插入值
                if (oneMinuteCount < 2) {
                    //插入值
                    Long time = nowTime;
                    Long value = 1L;
                    v.put(nowTime, value);
                    System.out.println("未超出1分钟限制，插入key=" + time + ",value=" + value);
                } else {
                    System.out.println("超出1分钟限制，不再插入请求记录");
                    beyondLimitFlag.set(true);
                }
                return v;
            });

            Long ttl = fastMap.ttl(phone);
            System.out.println("存活时间：" + ttl + "ms");
            if (ttl == null) {
                break;
            }
            Thread.sleep(1000L);
        }
    }
}
