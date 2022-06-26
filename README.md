# fastmap
支持等值查找、范围查找、数据过期等功能的Map，基于HashMap+TreeMap+ReentrantReadWriteLock实现。
## 核心思想
1.等值查找采用HashMap
2.范围查找采用TreeMap
3.数据过期实现：调用相关查询方法时清理过期Key + 定时（每秒）清理一遍过期Key
4.使用两个ReentrantReadWriteLock的读写锁实现线程安全，一个用于数据的CRUD，一个用于过期key的维护