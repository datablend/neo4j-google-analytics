package be.datablend.analytics.model;

import java.util.Date;

/**
 * User: dsuvee
 * Date: 08/03/12
 */
public class Navigation {

    public String source;
    public String target;
    public String targetTitle;
    public Date date;
    public long amount;

    public Navigation(String source, String target, String targetTitle, Date date, long amount) {
        this.source = source;
        this.target = target;
        this.targetTitle = targetTitle;
        this.date = date;
        this.amount = amount;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getTargetTitle() {
        return targetTitle;
    }

    public void setTargetTitle(String targetTitle) {
        this.targetTitle = targetTitle;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

}
