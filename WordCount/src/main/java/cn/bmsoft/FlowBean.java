package cn.bmsoft;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {

private  long upFlow;
private  long downFlow;
private  long sumFlow;

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }


    public long getUpFlow() {
        return upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public FlowBean(long upFlow, long downFlow) {

        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow+downFlow;
    }
    public void set(long upFlow, long downFlow) {

        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow+downFlow;
    }

    public FlowBean() {

    }

    @Override
    public String toString() {
        return "cn.bmsoft.FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sumFlow=" + sumFlow +
                '}';
    }

    public int compareTo(FlowBean o) {
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(upFlow);
    dataOutput.writeLong(downFlow);
    dataOutput.writeLong(sumFlow);



    }

    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();


    }
}
