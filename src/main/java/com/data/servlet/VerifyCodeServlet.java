package com.data.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class VerifyCodeServlet extends HttpServlet {


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        String code = req.getParameter("code");
        String key = (String) req.getSession().getAttribute("CODE");
        if(code != null && code.equalsIgnoreCase(key)){
            req.getSession().removeAttribute("CODE");
            resp.getWriter().println(true);
        }else{
            resp.getWriter().println(false);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // TODO Auto-generated method stub
        super.doGet(req, resp);
    }
}
