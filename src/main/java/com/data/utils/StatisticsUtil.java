package com.data.utils;

import com.google.common.base.Preconditions;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;

public class StatisticsUtil {

    /**
     * 功能描述:
     * 〈获取中位数〉
     *
     * @params : [numberList]
     * @return : java.util.List<java.lang.Integer>
     * @author : cwl
     * @date : 2019/11/4 14:40
     */
    public static List<Number> getMedian(List<? extends Number> numberList){
        if(null == numberList || numberList.size() == 0){
            throw new RuntimeException("numberList is not be empty or null");
        }
        List<Number> resultList = new ArrayList<>();
        if(numberList.size() % 2 == 0){
            int a = numberList.size()/2;
            int b = numberList.size()/2 - 1;
            resultList.add(numberList.get(a));
            resultList.add(numberList.get(b));
        }else{
            resultList.add(numberList.get(numberList.size()/2));
        }
        return resultList;
    }

    /**
     * 功能描述:
     * 〈获取平均值〉
     *
     * @params : [numberList]
     * @return : java.util.List<java.lang.Number>
     * @author : cwl
     * @date : 2019/11/4 14:45
     */
    public static Number getAverage(List<? extends Number> numberList){
        if(null == numberList || numberList.size() == 0){
            throw new RuntimeException("numberList is not be empty or null");
        }
        double v = getSum(numberList).doubleValue();
        return v / numberList.size();
    }

    /**
     * 功能描述:
     * 〈求和〉
     *
     * @params : [numberList]
     * @return : java.util.List<java.lang.Number>
     * @author : cwl
     * @date : 2019/11/4 14:47
     */
    public static Number getSum(List<? extends Number> numberList){
        if(null == numberList || numberList.size() == 0){
            throw new RuntimeException("numberList is not be empty or null");
        }
        Number result = new Number() {
            @Override
            public int intValue() {
                return 0;
            }

            @Override
            public long longValue() {
                return 0;
            }

            @Override
            public float floatValue() {
                return 0;
            }

            @Override
            public double doubleValue() {
                return 0;
            }
        };
        long longResult = 0;
        int intResult = 0 ;
        double doubleResult = 0;
        float floatResult = 0;
        for (Number number : numberList) {
            if(number.longValue() != 0){
                longResult = longResult + number.longValue();
            }
            if(number.doubleValue() != 0){
                doubleResult = doubleResult + number.doubleValue();
            }
            if(number.intValue() != 0){
                intResult = intResult + number.intValue();
            }
            if(number.floatValue() != 0){
                floatResult = floatResult + number.floatValue();
            }
        }
        return longResult + intResult + doubleResult + floatResult;

    }

    /**  生成不重复随机数
     * 根据给定的最小数字和最大数字，以及随机数的个数，产生指定的不重复的数组
     * @param begin 最小数字（包含该数）
     * @param end 最大数字（不包含该数）
     * @param size 指定产生随机数的个数
     */
    public int[] generateRandomNumber(int begin, int end, int size) {
        // 加入逻辑判断，确保begin<end并且size不能大于该表示范围
        if (begin >= end || (end - begin) < size) {
            return null;
        }
        // 种子你可以随意生成，但不能重复
        int[] seed = new int[end - begin];

        for (int i = begin; i < end; i ++) {
            seed[i - begin] = i;
        }
        int[] ranArr = new int[size];
        Random ran = new Random();
        // 数量你可以自己定义。
        for (int i = 0; i < size; i++) {
            // 得到一个位置
            int j = ran.nextInt(seed.length - i);
            // 得到那个位置的数值
            ranArr[i] = seed[j];
            // 将最后一个未用的数字放到这里
            seed[j] = seed[seed.length - 1 - i];
        }
        return ranArr;
    }


    /**  生成不重复随机数
     * 根据给定的最小数字和最大数字，以及随机数的个数，产生指定的不重复的数组
     * @param begin 最小数字（包含该数）
     * @param end 最大数字（不包含该数）
     * @param size 指定产生随机数的个数
     */
    public Integer[] generateBySet(int begin, int end, int size) {
        // 加入逻辑判断，确保begin<end并且size不能大于该表示范围
        if (begin >= end || (end - begin) < size) {
            return null;
        }

        Random ran = new Random();
        Set<Integer> set = new HashSet<Integer>();
        while (set.size() < size) {
            set.add(begin + ran.nextInt(end - begin));
        }

        Integer[] ranArr = new Integer[size];
        ranArr = set.toArray(new Integer[size]);
        //ranArr = (Integer[]) set.toArray();

        return ranArr;
    }
    /**
     * 判断String是否是整数
     */
    public boolean isInteger(String s){
        if((s != null)&&(s!=""))
            return s.matches("^[0-9]*$");
        else
            return false;
    }
    /**
     * 判断字符串是否是浮点数
     */
    public boolean isDouble(String value) {
        try {
            Double.parseDouble(value);
            if (value.contains("."))
                return true;
            return false;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    /**
     * 判断字符串是否是数字
     */
    public boolean isNumber(String value) {
        return isInteger(value) || isDouble(value);
    }

    //排序方法
    public static void sort(int[] array) {// 小到大的排序
        int temp = 0;
        for (int i = 0; i < array.length; i++) {
            for (int j = i; j < array.length; j++) {
                if (array[i] > array[j]) {
                    temp = array[i];
                    array[i] = array[j];
                    array[j] = temp;
                }
            }
        }
    }

    /**
     * 是否是质数
     */
    public static boolean isPrimes(int n) {
        for (int i = 2; i <= Math.sqrt(n); i++) {
            if (n % i == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * 阶乘
     * @param n
     * @return
     */
    public static int factorial(int n) {
        if (n == 1) {
            return 1;
        }
        return n * factorial(n - 1);
    }
    /**
     * 平方根算法
     * @param x
     * @return
     */
    public static long sqrt(long x) {
        long y = 0;
        long b = (~Long.MAX_VALUE) >>> 1;
        while (b > 0) {
            if (x >= y + b) {
                x -= y + b;
                y >>= 1;
                y += b;
            } else {
                y >>= 1;
            }
            b >>= 2;
        }
        return y;
    }

    private int math_subnode(int selectNum, int minNum) {
        if (selectNum == minNum) {
            return 1;
        } else {
            return selectNum * math_subnode(selectNum - 1, minNum);
        }
    }

    private int math_node(int selectNum) {
        if (selectNum == 0) {
            return 1;
        } else {
            return selectNum * math_node(selectNum - 1);
        }
    }
    /**
     * 可以用于计算双色球、大乐透注数的方法
     * selectNum：选中了的小球个数
     * minNum：至少要选中多少个小球
     * 比如大乐透35选5可以这样调用processMultiple(7,5);
     * 就是数学中的：C75=7*6/2*1
     */
    public int processMultiple(int selectNum, int minNum) {
        int result;
        result = math_subnode(selectNum, minNum)
                / math_node(selectNum - minNum);
        return result;
    }

    /**
     * 求m和n的最大公约数
     */
    public static int gongyue(int m, int n) {
        while (m % n != 0) {
            int temp = m % n;
            m = n;
            n = temp;
        }
        return n;
    }

    /**
     * 求两数的最小公倍数
     */
    public static int gongbei(int m, int n) {
        return m * n / gongyue(m, n);
    }

    /**
     * 递归求两数的最大公约数
     */
    public static int divisor(int m,int n){
        if(m%n==0){
            return n;
        }else{
            return divisor(n,m%n);
        }
    }

    /**
     * 默认精度
     */
    private static final int DEFAULT_SCALE = 64;
    private StatisticsUtil() {}

    public static BigDecimal variance(byte[] arr) {
        Preconditions.checkNotNull(arr);

        String[] strArr = new String[arr.length];
        for (int i = 0; i < arr.length; i++) {
            strArr[i] = String.valueOf(arr[i]);
        }
        return variance(strArr);
    }

    /**
     *  方差计算
     * @param arr
     * @param scale
     * @return
     */
    public static BigDecimal variance(byte[] arr, int scale) {
        Preconditions.checkNotNull(arr);
        Preconditions.checkArgument(scale > 0, "scale must be positive: " + scale);

        String[] strArr = new String[arr.length];
        for (int i = 0; i < arr.length; i++) {
            strArr[i] = String.valueOf(arr[i]);
        }
        return variance(strArr, scale);
    }

    /**
     * 方差计算
     * @param arr
     * @return
     */
    public static BigDecimal variance(char[] arr) {
        Preconditions.checkNotNull(arr);

        String[] strArr = new String[arr.length];
        for (int i = 0; i < arr.length; i++) {
            strArr[i] = String.valueOf(arr[i]);
        }
        return variance(strArr);
    }

    /**
     * 方差计算
     * @param arr
     * @param scale
     * @return
     */
    public static BigDecimal variance(char[] arr, int scale) {
        Preconditions.checkNotNull(arr);
        Preconditions.checkArgument(scale > 0, "scale must be positive: " + scale);



        String[] strArr = new String[arr.length];
        for (int i = 0; i < arr.length; i++) {
            strArr[i] = String.valueOf(arr[i]);
        }
        return variance(strArr, scale);
    }

    /**
     * 方差计算
     * @param arr
     * @return
     */
    public static BigDecimal variance(int[] arr) {
        Preconditions.checkNotNull(arr);

        String[] strArr = new String[arr.length];
        for (int i = 0; i < arr.length; i++) {
            strArr[i] = String.valueOf(arr[i]);
        }
        return variance(strArr);
    }

    /**
     * 方差计算
     * @param arr
     * @param scale
     * @return
     */
    public static BigDecimal variance(int[] arr, int scale) {
        Preconditions.checkNotNull(arr);
        Preconditions.checkArgument(scale > 0, "scale must be positive: " + scale);
        String[] strArr = new String[arr.length];
        for (int i = 0; i < arr.length; i++) {
            strArr[i] = String.valueOf(arr[i]);
        }
        return variance(strArr, scale);
    }

    /**
     * 方差计算
     * @param arr
     * @return
     */
    public static BigDecimal variance(long[] arr) {
        Preconditions.checkNotNull(arr);

        String[] strArr = new String[arr.length];
        for (int i = 0; i < arr.length; i++) {
            strArr[i] = String.valueOf(arr[i]);
        }
        return variance(strArr);
    }

    /**
     * 方差计算
     * @param arr
     * @param scale
     * @return
     */
    public static BigDecimal variance(long[] arr, int scale) {
        Preconditions.checkNotNull(arr);
        Preconditions.checkArgument(scale > 0, "scale must be positive: " + scale);

        String[] strArr = new String[arr.length];
        for (int i = 0; i < arr.length; i++) {
            strArr[i] = String.valueOf(arr[i]);
        }
        return variance(strArr, scale);
    }

    /**
     * 方差计算
     * @param arr
     * @return
     */
    public static BigDecimal variance(float[] arr) {
        Preconditions.checkNotNull(arr);

        String[] strArr = new String[arr.length];
        for (int i = 0; i < arr.length; i++) {
            strArr[i] = String.valueOf(arr[i]);
        }
        return variance(strArr);
    }

    /**
     * 方差计算
     * @param arr
     * @param scale
     * @return
     */
    public static BigDecimal variance(float[] arr, int scale) {
        Preconditions.checkNotNull(arr);
        Preconditions.checkArgument(scale > 0, "scale must be positive: " + scale);

        String[] strArr = new String[arr.length];
        for (int i = 0; i < arr.length; i++) {
            strArr[i] = String.valueOf(arr[i]);
        }
        return variance(strArr, scale);
    }

    /**
     * 方差计算
     * @param arr
     * @return
     */
    public static BigDecimal variance(double[] arr) {
        Preconditions.checkNotNull(arr);

        String[] strArr = new String[arr.length];
        for (int i = 0; i < arr.length; i++) {
            strArr[i] = String.valueOf(arr[i]);
        }
        return variance(strArr);
    }

    /**
     * 方差计算
     * @param arr
     * @param scale
     * @return
     */
    public static BigDecimal variance(double[] arr, int scale) {
        Preconditions.checkNotNull(arr);
        Preconditions.checkArgument(scale > 0, "scale must be positive: " + scale);

        String[] strArr = new String[arr.length];
        for (int i = 0; i < arr.length; i++) {
            strArr[i] = String.valueOf(arr[i]);
        }
        return variance(strArr, scale);
    }

    /**
     * 方差计算
     * @param arr
     * @return
     */
    private static BigDecimal variance(String[] arr) {
        return variance(arr, DEFAULT_SCALE);
    }


    /**
     * 方差计算
     * @param arr
     * @param scale
     * @return
     */
    private static BigDecimal variance(String[] arr, int scale) {
        if (arr.length < 2) {
            return BigDecimal.ZERO;
        }
        BigDecimal sum = BigDecimal.ZERO;
        for (int i = 0; i < arr.length; i++) {
            sum = sum.add(new BigDecimal(arr[i]));
        }
        BigDecimal meanNum = sum.divide(new BigDecimal(arr.length),
                scale, BigDecimal.ROUND_HALF_DOWN);
        BigDecimal tmp = null;
        BigDecimal tmpSum = BigDecimal.ZERO;
        for (int i = 0; i < arr.length; i++) {
            tmp = meanNum.subtract(new BigDecimal(arr[i]));
            tmpSum = tmpSum.add(tmp.multiply(tmp));
        }
        BigDecimal vari = tmpSum.divide(new BigDecimal(arr.length - 1),
                scale, BigDecimal.ROUND_HALF_DOWN);
        return new BigDecimal(trimZero(vari.toString()));
    }

    /**
     * 去除小数中后面多余的0
     *
     * @param str
     * @return
     */
    private static String trimZero(String str) {
        if (!str.contains(".")) {
            return str;
        }

        StringBuilder ret = new StringBuilder();
        char[] chars = str.toCharArray();
        // stop trimming 0
        boolean stopTrim = false;
        for (int i = chars.length - 1; i >= 0; i--) {
            char ch = chars[i];
            if (stopTrim) {
                ret.append(ch);
                continue;
            }

            // not stop trimming 0
            if (ch != '0') {
                ret.append(ch);
                stopTrim = true;
            }
        }
        if (ret.charAt(0) == '.') {
            ret.deleteCharAt(0);
        }
        return ret.reverse().toString();
    }



    /**
     *        计算两个二维数组的协方差
     * @param X
     * @param Y
     * @return
     */
    public double[][] doubleArr(double[][] X,double[][] Y){

        int xLen = X.length;
        int yLen = Y.length;

        double[][] covXY=new double[xLen][yLen];
        //创建一个相同的String数组，用来存放数据库
        String[][] c=new String[xLen][yLen];
        for (int i=0; i<xLen; i++){
            double avgX = calculateAvg( X[i]);
            for (int j=0; j<yLen; j++){
                //求数组平均值
                double avgY = calculateAvg( Y[j]);
                double avgXY = calculateMultiplyAvg( X[i], Y[j]);
                //相乘之后的平均值 减去 数组平均值相乘
                double result = avgXY - (avgX * avgY);
                covXY[i][j]=result;
                //格式化科学计数法
                DecimalFormat decimalFormat = new DecimalFormat("#,##0.000000");//格式化设置
                String format = decimalFormat.format(covXY[i][j]);
                c[i][j] = format;

            }
        }
        return covXY;
    }

    /**
     *        求数组平均值
     * @param arr  传一个一维数组
     * @return
     */
    public double calculateAvg(double arr[])
    {
        double avg=0;
        for(int i=0; i<arr.length; i++)
        {
            avg += arr[i];
        }
        //数组里面的数据相加求和，再求平均数
        avg=avg / arr.length;
        return avg;
    }

    /**
     *       求XY相乘之后的平均值
     * @param x
     * @param y
     * @return
     */
    public  double calculateMultiplyAvg(double x[], double y[]){
        int len = x.length;
        double a[] = new double[len];
        for(int i=0; i<x.length; i++){
            if(i<y.length){
                //先把两个数组的同角标相乘，放到新的数组中
                a[i] = x[i] * y[i];
            }
        }
        //调用求平均值的方法
        return calculateAvg(a);
    }


    public static void main(String[] args){
        StatisticsUtil util=new StatisticsUtil();
        System.out.println(util.sqrt(100));
    }



}
