package com.bxg.test;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/*
* {"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"
* */
public class RateBean implements  WritableComparable<RateBean> {
    private     String movie;
    private     String rate;
    private     String timeStamp;
    private     String uid;

    public RateBean() {
    }

    public RateBean(String movie, String rate, String timeStamp, String uid) {
        this.movie = movie;
        this.rate = rate;
        this.timeStamp = timeStamp;
        this.uid = uid;
    }

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public String getRate() {
        return rate;
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public int compareTo(RateBean o) {
        int num = new Integer(o.rate)-new Integer(this.rate);
        return num ;

    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(movie);
        out.writeUTF(rate);
        out.writeUTF(timeStamp);
        out.writeUTF(uid);
    }

    public void readFields(DataInput in) throws IOException {
        this.movie = in.readUTF();
        this.rate    =in.readUTF();
        this.timeStamp =in.readUTF();
        this.uid =in.readUTF();
    }

    @Override
    public String toString() {
        return "RateBean{" +
                "movie='" + movie + '\'' +
                ", rate='" + rate + '\'' +
                ", timeStamp='" + timeStamp + '\'' +
                ", uid='" + uid + '\'' +
                '}';
    }


//    @Override
//    public int hashCode() {
//        final int prime = 31;
//        int result = 17;
//        result = prime * result + ((movie == null) ? 0 : movie.hashCode());
//        result = prime * result + ((rate == null) ? 0 : rate.hashCode());
//        result = prime * result + ((timeStamp == null) ? 0 : timeStamp.hashCode());
//        result = prime * result + ((uid == null) ? 0 : uid.hashCode());
//        return result;
//    }


//    @Override
//    public boolean equals(Object obj) {
//
//        RateBean other = (RateBean) obj;
//        return  uid.equals(other.uid) && timeStamp.equals(other.timeStamp)
//                && rate.equals(other.rate) && movie.equals(other.movie);
//
//    }


//    public static void main(String[] args) throws Exception {
//        RateBean r1 = new RateBean("3105","5","978301713","1");
//        RateBean r2 = new RateBean("2687","3","978824268","1");
//        System.out.println(r1.hashCode()+"  "+ r2.hashCode());
//        System.out.println(r1.equals(r2));
//    }


}
