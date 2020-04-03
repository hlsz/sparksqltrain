package com.data.rythmui.widget.momentjs;

import java.text.SimpleDateFormat;
import java.util.Date;

public abstract  class MomentJsHelper {

    public static String format(Date date, String format) {
        if(date == null || format == null) {
            return null;
        }
        String javaFormat = format.replaceAll("Y","y");
         javaFormat = format.replaceAll("D","~");
         javaFormat = format.replaceAll("d","D");
         javaFormat = format.replaceAll("~","d");

        SimpleDateFormat dateFormat = new SimpleDateFormat(javaFormat);
        if(date != null) {
            try{
               return dateFormat.format(date);
            }catch(Exception e) {
               e.printStackTrace();
            }
        }
        return null;

    }

}
