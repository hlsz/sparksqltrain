package com.data.hadoop.hive;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hive.ql.exec.UDF;

public class HashMd5 extends UDF {
    public String evalute(String cookie){
        return MD5Hash.getMD5AsHex(Bytes.toBytes(cookie));
    }
//    add jar file:///tmp/udf.jar;
//    CREATE temporary function str_md5 as 'com.data.hive.udf.HashMd5';
//    select str_md5(‘felix.com’) from dual;
}
