package com.data.web.captcha;

import java.awt.*;

public interface CaptchaBuilder<T> {


    String DEFAULT_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    int DEFAULT_WIDTH = 88;

    int DEFAULT_HEIGHT = 31;

    int DEFAULT_CHAR_NUM  =4;

    Color DEFAULT_BG_COLOR = Color.GRAY;


    /**
     * 生成一个验证码
     * @return
     */
    Captcha<T> build();




}
