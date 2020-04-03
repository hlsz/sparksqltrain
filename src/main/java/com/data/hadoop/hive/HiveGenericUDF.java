package com.data.hadoop.hive;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 该函数用于将字符串切分成List，并返回
 */
public class HiveGenericUDF extends GenericUDF {


        private static int mapTasks = 0;
        private static String init = "";
        private transient ArrayList ret = new ArrayList();

        @Override
        public void configure(MapredContext context) {
            System.out.println(new Date() + "########   configure");
            if(null != context) {
                //从jobConf中获取map数
                mapTasks = context.getJobConf().getNumMapTasks();
            }
            System.out.println(new Date() +  "########   mapTasks [" + mapTasks + "] ..");
        }

        @Override
        public ObjectInspector initialize(ObjectInspector[] arguments)
                throws UDFArgumentException {
            System.out.println(new Date() +  "########   initialize");
            //初始化文件系统，可以在这里初始化读取文件等
            init = "init";
            //定义函数的返回类型为java的List
            ObjectInspector returnOI = PrimitiveObjectInspectorFactory
                    .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
            return ObjectInspectorFactory.getStandardListObjectInspector(returnOI);
        }

        @Override
        public Object evaluate(DeferredObject[] args) throws HiveException {
            ret.clear();
            if(args.length < 1) return ret;
            //获取第一个参数
            String str = args[0].get().toString();
            String[] s = str.split(",",-1);
            for(String word : s) {
                ret.add(word);
            }
            return ret;
        }

        @Override
        public String getDisplayString(String[] children) {
            return "Usage: HiveGenericUDF(String str)";
        }

}
