package com.data.spark.Bean;

import java.io.Serializable;

public class AppAccessLogInfo implements Serializable{

    private static final long serialVersionUID = 2298114085058810487L;

    private Long timeStamp;
    private Long upTraffic;
    private Long downTraffic;

    public AppAccessLogInfo() {}

    public AppAccessLogInfo(Long timeStamp, Long upTraffic, Long downTraffic) {
        this.timeStamp = timeStamp;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }
    public Long getUpTraffic() {
        return upTraffic;
    }

    public void setUpTraffic(Long upTraffic) {
        this.upTraffic = upTraffic;
    }

    public Long getDownTraffic() {
        return downTraffic;
    }

    public void setDownTraffic(Long downTraffic) {
        this.downTraffic = downTraffic;
    }

}

