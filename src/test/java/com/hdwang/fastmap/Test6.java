package com.hdwang.fastmap;

import java.util.*;

/**
 * 排序功能测试
 *
 * @author wanghuidong
 * 时间： 2022/7/2 20:57
 */
public class Test6 {


    public static void main(String[] args) {
        System.out.println("使用comparator测试排序");
        List<User> users = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            User user = new User();
            user.name = "name" + i;
            user.phone = "1301234110" + i;
            users.add(user);
        }
        //乱序
        Collections.shuffle(users);
        System.out.println(users);

        //使用排序器
        IFastMap<User, String> userPhoneMap = new FastMap<>(false, new UserComparator());
        for (User user : users) {
            userPhoneMap.put(user, user.phone);
        }
        System.out.println(userPhoneMap.keySet());

        System.out.println("使用自然序测试排序");
        List<Person> persons = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Person person = new Person();
            person.name = "name" + i;
            person.phone = "1301234110" + i;
            persons.add(person);
        }
        //乱序
        Collections.shuffle(persons);
        System.out.println(persons);

        //使用自然序，对象必须实现Comparable接口
        IFastMap<Person, String> personPhoneMap = new FastMap<>(false, true);
        for (Person person : persons) {
            personPhoneMap.put(person, person.phone);
        }
        System.out.println(personPhoneMap.keySet());
    }


    static class User {
        public String name;
        public String phone;

        @Override
        public String toString() {
            return "User{" +
                    "name='" + name + '\'' +
                    ", phone='" + phone + '\'' +
                    '}';
        }
    }

    static class UserComparator implements Comparator<User> {

        @Override
        public int compare(User o1, User o2) {
            return o1.name.compareTo(o2.name);
        }
    }


    static class Person implements Comparable<Person> {
        public String name;
        public String phone;

        @Override
        public String toString() {
            return "User{" +
                    "name='" + name + '\'' +
                    ", phone='" + phone + '\'' +
                    '}';
        }

        @Override
        public int compareTo(Person o) {
            return this.name.compareTo(o.name);
        }
    }
}
