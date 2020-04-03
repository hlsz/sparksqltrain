package com.data.common;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class XssFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws ServletException ,
            IOException  {
        HttpServletRequest request = (HttpServletRequest) req;
        HttpServletResponse response = (HttpServletResponse) res;
        // 禁止缓存
        response.setHeader("Cache-Control", "no-store");
        response.setHeader("Pragrma","no-cache");
        response.setDateHeader("Expires",0);
        //链接来源地址
        String referer = request.getHeader("referer");
        System.out.println("refer is "+referer);
        System.out.println("serverName is "+ request.getServerName());
        if (referer == null || !referer.contains(request.getServerName())){
            /**
             *如果 链接地址来自其它网站
             */
            request.getRequestDispatcher("/error.jsp").forward(request, response);
        }else{
            chain.doFilter(request, response);
        };
    }

    @Override
    public void destroy() {

    }
}
