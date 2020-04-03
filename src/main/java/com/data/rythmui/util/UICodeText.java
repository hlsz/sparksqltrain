package com.data.rythmui.util;

import java.io.Serializable;

public interface UICodeText extends Serializable {

    String getCode();

    String getText();

    public static class Impl implements UICodeText {

        private static final long serialVersionUID = 1L;
        private String code = "";
        private String text = "";

        public Impl() {
        }

        public Impl(String code, String text) {
            this.code = (code == null ) ? "" : code;
            this.text = (text == null) ? "" : text;
        }

        @Override
        public String getCode() {
            return null;
        }

        @Override
        public String getText() {
            return null;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public void setText(String text) {
            this.text = text;
        }
    }
}
