package utils;

import java.io.Serializable;

public class ExpireTime implements Serializable {
    private int days;
    private int weeks;
    private int months;
    private int years;

    private static final long serialVersionUID = -8078265238403408325L;

    public ExpireTime() {
        this.days = 0;
        this.weeks = 0;
        this.months = 0;
        this.years = 0;
    }

    public void setDays(int days) {
        this.days = days;
    }

    public void setWeeks(int weeks) {
        this.weeks = weeks;
    }

    public void setMonths(int months) {
        this.months = months;
    }

    public void setYears(int years) {
        this.years = years;
    }

    public int getDays() {
        return this.days;
    }

    public int getWeeks() {
        return this.weeks;
    }

    public int getMonths() {
        return this.months;
    }

    public int getYears() {
        return this.years;
    }
}