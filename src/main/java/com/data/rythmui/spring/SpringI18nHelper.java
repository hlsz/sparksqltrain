package com.data.rythmui.spring;

import clojure.lang.Obj;
import org.springframework.context.ApplicationContext;
import org.springframework.context.i18n.LocaleContextHolder;

public abstract class SpringI18nHelper {

    public static  final String getI18nMessage(String code, Object... args){
        ApplicationContext applicationContext = SpringHolder.getApplicationContext();
        return applicationContext.getMessage(code, args, LocaleContextHolder.getLocale());
    }
}
