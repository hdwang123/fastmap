package com.hdwang.fastmap;

import java.text.ParseException;
import java.text.SimpleDateFormat;

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
        //记录每次访问记录：key:时间戳，value:访问次数
        IFastMap<Long, Long> innerFastMap = new FastMap<>(false, true);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        innerFastMap.put(dateFormat.parse("2022-06-26 16:00:00").getTime(), 1L);
//        innerFastMap.put(dateFormat.parse("2022-06-26 16:00:30").getTime(), 1L);
//        innerFastMap.put(dateFormat.parse("2022-06-26 16:00:40").getTime(), 1L);
//        innerFastMap.put(dateFormat.parse("2022-06-26 16:01:00").getTime(), 1L);
//        innerFastMap.put(dateFormat.parse("2022-06-26 16:02:00").getTime(), 1L);
//        innerFastMap.put(dateFormat.parse("2022-06-26 16:03:00").getTime(), 1L);
        String phone = "13312340001";

        //put原子操作，线程安全，用来设置值
//        fastMap.put(phone, innerFastMap);

        //compute原子操作，线程安全，可以用来初始化指定key的值和计算指定key新的值
        fastMap.compute(phone, (k, v) -> {
            if (v == null) {
                v = new FastMap<>(false, true);
            }
            try {
                v.put(dateFormat.parse("2022-06-26 16:00:00").getTime(), 1L);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return v;
        });
        System.out.println("当前时间；" + System.currentTimeMillis());

        //过期时间设置5分钟
        Long expireTime = fastMap.expire(phone, 5 * 60 * 1000L);
        System.out.println("过期时间；" + expireTime);

        //统计次数
        IFastMap<Long, Long> timeCountMap = fastMap.get(phone);
        Long startTime = timeCountMap.firstKey(); //首次记录
        Long endTime = startTime + 1 * 60 * 1000L;
        long oneMinuteCount = timeCountMap.subMap(startTime, endTime).size();
        System.out.println("oneMinuteCount:" + oneMinuteCount);

        endTime = startTime + 5 * 60 * 1000L;
        long fiveMinuteCount = timeCountMap.subMap(startTime, endTime).size();
        System.out.println("fiveMinuteCount:" + fiveMinuteCount);

        while (true) {
            Long ttl = fastMap.ttl(phone);
            System.out.println("存活时间：" + ttl + "ms");
            if (ttl == null) {
                break;
            }
            Thread.sleep(1000L);
        }
    }
}
