package com.data.rythmui.util;

import org.apache.taglibs.standard.tag.common.core.ImportSupport;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public  abstract class UrlHelper {
    public static String resolveUrlWithContextPath(String url, HttpServletRequest request) {
        if (url == null) {
            return "";
        }
        if (ImportSupport.isAbsoluteUrl(url)) {
            return url;
        }
        if (request == null) {
            request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
        }
        if (url.startsWith("/")) {
            return (request.getContextPath() + url);
        }
        return request.getContextPath() + "/" + url;
    }
}
