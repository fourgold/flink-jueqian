package com.ecust.example;

import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * @author JueQian
 * @create 01-20 18:55
 * 测试java8新特性
 */
public class Flink02_Require_TimeOut_TestNew {
    public static void main(String[] args) {
        ArrayList<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");

        String collect = list.stream().collect(Collectors.joining("->"));
        System.out.println(collect);
        //a->b->c
    }
}
