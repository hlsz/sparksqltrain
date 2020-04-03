package com.data.web.captcha;

public interface Captcha<T> {

    /**
     * 获得验证码文字
     * @return
     */
    String getCode();

    /**
     * 获得验证码对象
     * @return
     */
    T getCaptcha();
}
