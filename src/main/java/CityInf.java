import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CityInf implements WritableComparable<CityInf> {
    //key
    private DoubleWritable longitude;
    private DoubleWritable latitude;

    private Text city;
    private IntWritable population;

    public int compareTo(CityInf o) {
        //比较
        return (longitude.compareTo(o.longitude)!=0)
                ?longitude.compareTo(o.longitude)
                :latitude.compareTo(o.latitude);
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //序列化
        longitude.write(dataOutput);
        latitude.write(dataOutput);
        city.write(dataOutput);
        population.write(dataOutput);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //反序列化
        longitude.readFields(dataInput);
        latitude.readFields(dataInput);
        city.readFields(dataInput);
        population.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CityInf)) return false;
        CityInf cityInf = (CityInf) o;
        return longitude.equals(cityInf.longitude) &&
                latitude.equals(cityInf.latitude);
    }

    @Override
    public int hashCode() {
        return Objects.hash(longitude, latitude);
    }

    @Override
    public String toString() {
        return longitude+","
                +latitude+","
                +city+","
                +population;
    }

    //
    //set get 和 构造函数
    //
    public CityInf(DoubleWritable longitude, DoubleWritable latitude, Text city, IntWritable population) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.city = city;
        this.population = population;
    }

    public DoubleWritable getLongitude() {
        return longitude;
    }

    public void setLongitude(DoubleWritable longitude) {
        this.longitude = longitude;
    }

    public DoubleWritable getLatitude() {
        return latitude;
    }

    public void setLatitude(DoubleWritable latitude) {
        this.latitude = latitude;
    }

    public Text getCity() {
        return city;
    }

    public void setCity(Text city) {
        this.city = city;
    }

    public IntWritable getPopulation() {
        return population;
    }

    public void setPopulation(IntWritable population) {
        this.population = population;
    }
}
