package com.data.web.captcha;

import java.awt.image.BufferedImage;

public class BufferedImageCaptcha implements Captcha<BufferedImage>{

    private String code;

    private BufferedImage captcha;

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public BufferedImage getCaptcha() {
        return captcha;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public void setCaptcha(BufferedImage captcha) {
        this.captcha = captcha;
    }
}
