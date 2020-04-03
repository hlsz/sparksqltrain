package com.data.model;

import java.util.Date;

public class Days {

    private int holidayId;

    private String holidayName;

    private Date holidayDate;

    public Days(int holidayId, String holidayName, Date holidayDate) {
        this.holidayId = holidayId;
        this.holidayName = holidayName;
        this.holidayDate = holidayDate;
    }

    public int getHolidayId() {
        return holidayId;
    }

    public void setHolidayId(int holidayId) {
        this.holidayId = holidayId;
    }

    public String getHolidayName() {
        return holidayName;
    }

    public void setHolidayName(String holidayName) {
        this.holidayName = holidayName;
    }

    public Date getHolidayDate() {
        return holidayDate;
    }

    public void setHolidayDate(Date holidayDate) {
        this.holidayDate = holidayDate;
    }

    public Date getDate() { return new Date();
    }

    public String getTitle() { return holidayName;
    }
}
