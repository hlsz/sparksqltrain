package com.data.dao;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ProductData {

    String randomIp;
    String randomPhoneNumber;

    public String getRandomIp() {
        return randomIp;
    }

    public void setRandomIp(String randomIp) {
        this.randomIp = randomIp;
    }

    public String getRandomPhoneNumber() {
        return randomPhoneNumber;
    }

    public void setRandomPhoneNumber(String randomPhoneNumber) {
        this.randomPhoneNumber = randomPhoneNumber;
    }

    public ProductData() {
    }

    public ProductData(String randomIp, String randomPhoneNumber) {
        this.randomIp = randomIp;
        this.randomPhoneNumber = randomPhoneNumber;
    }

    public String getRecentAMonthRandomTime(String format){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.format(new Date());
        return sdf.toString();
    }
}
