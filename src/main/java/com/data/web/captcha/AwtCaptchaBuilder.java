package com.data.web.captcha;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.util.Random;

public class AwtCaptchaBuilder implements CaptchaBuilder<BufferedImage>{

    private String chars = DEFAULT_CHARS;
    private int  charNum = DEFAULT_CHAR_NUM;
    private int width = DEFAULT_WIDTH;
    private int height = DEFAULT_HEIGHT;
    private int paddingTop = 5;
    private int paddingRight = 2;
    private int paddingButtom = 5;
    private int paddingLeft = 2;
    private int lineNum = 5;
    private Color bgColor = null;
    private int fontSize = 0;
    private String fontName = null;
    private boolean isBold = true;
    private boolean isItalic = true;




    @Override
    public Captcha<BufferedImage> build() {
        StringBuilder sb = new StringBuilder();
        BufferedImage image = new BufferedImage(this.width, this.height, BufferedImage.TYPE_INT_RGB);

        Graphics graphics = image.getGraphics();

        graphics.setColor(bgColor == null ? DEFAULT_BG_COLOR: this.bgColor);

        graphics.fillRect(0, 0, this.width, this.height);

        Random random = new Random();

        int realFontSizePx = calcFontSizePx();

        int realFontSizePt = (int)(realFontSizePx / 0.75);
        for (int i = 0; i < this.charNum; i++) {

            int n = random.nextInt(chars.length());

            graphics.setColor(getRandomColor());

            graphics.setFont(new Font(fontName,
                    (isBold ? Font.BOLD: Font.PLAIN) + (isItalic? Font.ITALIC: Font.PLAIN), realFontSizePt));

            graphics.drawString(chars.charAt(n) + "", calcDrawX(i, realFontSizePx), calcDrawY(realFontSizePx));

            sb.append(chars.charAt(n));

        }

        for (int i = 0; i < this.lineNum; i++) {
            graphics.setColor(getRandomColor());
            graphics.drawLine(random.nextInt(this.width), random.nextInt(this.height),
                    random.nextInt(this.width), random.nextInt(this.height));
        }
        BufferedImageCaptcha captcha = new BufferedImageCaptcha();
        captcha.setCode(sb.toString());
        captcha.setCaptcha(image);
        return captcha;
    }


    private int calcDrawX(int i, int fontSize){
        return this.paddingLeft + i * (this.width - this.paddingLeft - this.paddingRight) / this.charNum;
    }

    private int calcDrawY(int fontSize){
        return (this.height + fontSize) / 2;
    }

    private int calcFontSizePx(){
        if(this.fontSize > 0){
            return this.fontSize;
        }else if(this.fontSize == 0){
            return Math.max(this.height - this.paddingButtom - this.paddingTop, 8);
        } else {
            return Math.max((this.width - this.paddingLeft - this.paddingRight) / this.charNum, 8);
        }
    }

    public static Color getRandomColor(){
        Random random = new Random();
        Color color= new Color(random.nextInt(256), random.nextInt(256), random.nextInt(256));
        return color;
    }


    public String getChars() {
        return chars;
    }

    public void setChars(String chars) {
        this.chars = chars;
    }

    public int getCharNum() {
        return charNum;
    }

    public void setCharNum(int charNum) {
        this.charNum = charNum;
    }

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public int getPaddingTop() {
        return paddingTop;
    }

    public void setPaddingTop(int paddingTop) {
        this.paddingTop = paddingTop;
    }

    public int getPaddingRight() {
        return paddingRight;
    }

    public void setPaddingRight(int paddingRight) {
        this.paddingRight = paddingRight;
    }

    public int getPaddingButtom() {
        return paddingButtom;
    }

    public void setPaddingButtom(int paddingButtom) {
        this.paddingButtom = paddingButtom;
    }

    public int getPaddingLeft() {
        return paddingLeft;
    }

    public void setPaddingLeft(int paddingLeft) {
        this.paddingLeft = paddingLeft;
    }

    public int getLineNum() {
        return lineNum;
    }

    public void setLineNum(int lineNum) {
        this.lineNum = lineNum;
    }

    public Color getBgColor() {
        return bgColor;
    }

    public void setBgColor(Color bgColor) {
            this.bgColor = bgColor;
    }

    public void setBgColor(String bgColor) {
        if(bgColor == null){
            return ;
        }
        if (bgColor.matches("^((#)|(0[x|X]))([0-9]|[A-F]|[a-f]){6}$")) {
            this.bgColor = new Color(Integer.decode(bgColor));
        }
    }

    public int getFontSize() {
        return fontSize;
    }

    public void setFontSize(int fontSize) {
        this.fontSize = fontSize;
    }

    public String getFontName() {
        return fontName;
    }

    public void setFontName(String fontName) {
        this.fontName = fontName;
    }

    public boolean isBold() {
        return isBold;
    }

    public void setBold(boolean bold) {
        isBold = bold;
    }

    public boolean isItalic() {
        return isItalic;
    }

    public void setItalic(boolean italic) {
        isItalic = italic;
    }
}
