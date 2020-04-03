package com.data.rythmui.widget.bootstrapselect;

public class SelectOptionUnit implements SelectOption {

    private String value = null;
    private String text = null;
    private String label = null;
    private String labelCss = null;
    private String icon = null;
    private String content = null;
    private String tokens = null;
    private String clazz = null;
    private boolean disabled = false;

    public SelectOptionUnit() {
    }

    public SelectOptionUnit(String value, String text) {
        this.value = value;
        this.text = text;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabelCss() {
        return labelCss;
    }

    public void setLabelCss(String labelCss) {
        this.labelCss = labelCss;
    }

    public String getIcon() {
        return icon;
    }

    public void setIcon(String icon) {
        this.icon = icon;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getTokens() {
        return tokens;
    }

    public void setTokens(String tokens) {
        this.tokens = tokens;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }
}
