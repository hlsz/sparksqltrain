package com.data.servlet;

import com.data.utils.VerifyCodeUtils;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class CreateCodeServlet extends HttpServlet {


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        //设置相应类型,告诉浏览器输出的内容为图片
        resp.setContentType("image/jpeg");
        //设置响应头信息，告诉浏览器不要缓存此内容
        resp.setHeader("Pragma", "No-cache");
        resp.setHeader("Cache-Control", "no-cache");
        resp.setDateHeader("Expire", 0);

        //生成随机字串
        String verifyCode = VerifyCodeUtils.generateVerifyCode(4);
        //存入会话session
        req.getSession().setAttribute("CODE", verifyCode.toLowerCase());

        //生成图片
        int width = 100;//宽
        int height = 40;//高
        VerifyCodeUtils.outputImage(width, height, resp.getOutputStream(), verifyCode);

    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // TODO Auto-generated method stub
        doGet(req, resp);
    }
}
