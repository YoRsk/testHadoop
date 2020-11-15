package shiyan6;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NumWritable implements Writable {
    private long sum;
    private long max;
    private long min;
    private long count;
    private double avg;

    public NumWritable(){
    }
    public long Sum(long value){
        this.sum+=value;
        return this.sum;
    }

    public long Max(long value){
        if(this.max < value){
            this.max = value;
        }
        return this.max;
    }
    public long Min(long value){
        if(this.min > value||this.min == 0){
            this.min = value;
        }
        return this.min;
    }
    public long Count(){
        this.count++;
        return this.count;
    }
    public double Avg(){
        this.avg = (double)this.sum/this.count;
        return this.avg;
    }

    @Override
    public String toString() {
        return  "sum " + this.sum + "\n" +
                "max " + this.max + "\n" +
                "min " + this.min + "\n" +
                "avg " + this.avg ;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(sum);
        dataOutput.writeLong(max);
        dataOutput.writeLong(min);
        dataOutput.writeLong(count);
        dataOutput.writeDouble(avg);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.sum = dataInput.readLong();
        this.max = dataInput.readLong();
        this.min = dataInput.readLong();
        this.count = dataInput.readLong();
        this.avg = dataInput.readDouble();
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public long getMin() {
        return min;
    }

    public void setMin(long min) {
        this.min = min;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }
}
