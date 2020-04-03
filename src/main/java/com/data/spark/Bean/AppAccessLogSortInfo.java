package com.data.spark.Bean;

import scala.math.Ordered;

import java.io.Serializable;

/**
 *      need implements Ordered interface and Serializable interface
 *
 */
public class AppAccessLogSortInfo implements Ordered<AppAccessLogSortInfo>, Serializable {

    private static final long serialVersionUID = 7006437160384780829L;

    private Long timeStamp;
    private Long upTraffic;
    private Long downTraffic;

    public AppAccessLogSortInfo() {}

    public AppAccessLogSortInfo(Long timeStamp, Long upTraffic, Long downTraffic) {
        super();
        this.timeStamp = timeStamp;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }

    @Override
    public boolean $greater(AppAccessLogSortInfo other) {
        if (upTraffic > other.upTraffic) {
            return true;
        } else if (upTraffic == other.upTraffic && downTraffic > other.downTraffic) {
            return true;
        } else if (upTraffic == other.upTraffic && downTraffic == other.downTraffic && timeStamp > other.timeStamp) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(AppAccessLogSortInfo other) {
        if($greater(other)) {
            return true;
        } else if (upTraffic == other.upTraffic && downTraffic == other.downTraffic && timeStamp == other.timeStamp){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less(AppAccessLogSortInfo other) {
        if(upTraffic < other.upTraffic) {
            return true;
        } else if (upTraffic == other.upTraffic && downTraffic < other.downTraffic) {
            return true;
        } else if (upTraffic == other.upTraffic && downTraffic == other.downTraffic && timeStamp < other.timeStamp) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(AppAccessLogSortInfo other) {
        if($less(other)) {
            return true;
        } else if (upTraffic == other.upTraffic && downTraffic == other.downTraffic && timeStamp == other.timeStamp) {
            return true;
        }
        return false;
    }

    @Override
    public int compare(AppAccessLogSortInfo other) {

        int timeStampGap = (int) (timeStamp - other.timeStamp);
        int upTrafficGap = (int) (upTraffic - other.upTraffic);
        int downTrafficGap = (int) (downTraffic - other.downTraffic);

        if(upTrafficGap != 0) {
            return upTrafficGap;
        } else if (downTrafficGap != 0) {
            return downTrafficGap;
        } else if (timeStampGap != 0) {
            return timeStampGap;
        }
        return 0;
    }

    @Override
    public int compareTo(AppAccessLogSortInfo other) {

        int timeStampGap = (int) (timeStamp - other.timeStamp);
        int upTrafficGap = (int) (upTraffic - other.upTraffic);
        int downTrafficGap = (int) (downTraffic - other.downTraffic);

        if(upTrafficGap != 0) {
            return upTrafficGap;
        } else if (downTrafficGap != 0) {
            return downTrafficGap;
        } else if (timeStampGap != 0) {
            return timeStampGap;
        }
        return 0;
    }

    public Long getTimpStamp() {
        return timeStamp;
    }

    public void setTimpStamp(Long timpStamp) {
        this.timeStamp = timpStamp;
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((downTraffic == null) ? 0 : downTraffic.hashCode());
        result = prime * result + ((timeStamp == null) ? 0 : timeStamp.hashCode());
        result = prime * result + ((upTraffic == null) ? 0 : upTraffic.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AppAccessLogSortInfo other = (AppAccessLogSortInfo) obj;
        if (downTraffic == null) {
            if (other.downTraffic != null)
                return false;
        } else if (!downTraffic.equals(other.downTraffic))
            return false;
        if (timeStamp == null) {
            if (other.timeStamp != null)
                return false;
        } else if (!timeStamp.equals(other.timeStamp))
            return false;
        if (upTraffic == null) {
            if (other.upTraffic != null)
                return false;
        } else if (!upTraffic.equals(other.upTraffic))
            return false;
        return true;
    }

}
