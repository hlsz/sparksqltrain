package com.data.demo;

import java.util.*;
import java.util.stream.Stream;

class Employee {
    private Long id;
    private String name;

    public Employee(long id, String name) {
        this.id = id;
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}


public class Java8Maps {

    private static Map<String, Employee > map1 = new HashMap<>();
    private static Map<String, Employee > map2 = new HashMap<>();


    public static void main(String[] args) {

        Employee employee1 = new Employee(1L, "Henry");
        map1.put(employee1.getName(), employee1);
        Employee employee2 = new Employee(22L, "Annie");
        map1.put(employee2.getName(), employee2);
        Employee employee3 = new Employee(8L, "John");
        map1.put(employee3.getName(), employee3);

        Employee employee4 = new Employee(2L, "George");
        map2.put(employee4.getName(), employee4);
        Employee employee5 = new Employee(3L, "Henry");
        map2.put(employee5.getName(), employee5);


        Map<String, Object> map33 = new HashMap<>(map1);

//        map2.forEach(
//                (key, value) -> map3.merge(key, value, (v1, v2) -> new Employee(v1.getId(), v2.getName())));

        Stream combined = Stream.concat(map1.entrySet().stream(), map2.entrySet().stream());
//        Map<String, Employee> result = combined.collect(
//                Collectors.toMap(Map.Entry::getKey,
//                        Map.Entry::getValue,
//                        (v1, v2) -> new Employee(v2.getId(), v1.getName())));


        //两个map具有不同的key
        HashMap map1=new HashMap();
        map1.put("clientId", "A001");

        HashMap map11 = new HashMap();
        map11.put("gradeCode","code001");
        map11.put("productName",new ArrayList<String>(){{add("a");add("b");add("c");}});
        map11.put("eventName",new ArrayList<>(Collections.nCopies(5, 1)));
        map11.put("serviceName", new ArrayList(Arrays.asList("c","d","e")));
        map1.put("gradeMap", map11);
        System.out.println(map1);

        HashMap map2 = new HashMap();
        map2.put("clientId", "A001");

        HashMap map12 = new HashMap();
        map12.put("classifyCode","code001");
        map12.put("productName",new ArrayList<String>(){{add("a");add("b");add("c");}});
        map2.put("classifyMap", map12);
        System.out.println(map2);

        map1.putAll(map2);
        System.out.println(map1);
        //两个map具有重复的key
//        HashMap map3=new HashMap();
//        map3.put("1", "A");
//        HashMap map4 = new HashMap();
//        map4.put("1", "B");
//        map4.put("3", "C");
//        map3.putAll(map4);
//        System.out.println(map3);













    }
}
