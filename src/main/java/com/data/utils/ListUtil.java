package com.data.utils;

import java.util.*;
import java.util.stream.Collectors;

public class ListUtil {

    // 删除ArrayList中重复元素，保持顺序
    public static void removeDuplicateWithOrder(List list) {
        Set set = new HashSet();
        List newList = new ArrayList();
        for (Iterator iter = list.iterator(); iter.hasNext();) {
            Object element = iter.next();
            if (set.add(element))
                newList.add(element);
        }
        list.clear();
        list.addAll(newList);
        System.out.println( " remove duplicate " + list);

        // 用JDK1.8 Stream中对List进行去重：list.stream().distinct();
        list = (List) list.stream().distinct().collect(Collectors.toList());
    }




}
