package Codes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class LinkedMR {

    private static final Text TEXT_SUM = new Text("SUM");
    private static final Text TEXT_COUNT = new Text("COUNT");
    private static final Text TEXT_AVG = new Text("AVG");

    // 计算SumMapper，多单个Map中的数据求和
    public static class SumMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        public long sum = 0;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            sum += Long.parseLong(value.toString());
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(TEXT_SUM, new LongWritable(sum));
        }
    }

    // 计算SumReducer，求和
    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        public long sum = 0;

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            for (LongWritable v : values) {
                sum += v.get();
            }
            context.write(TEXT_SUM, new LongWritable(sum));
        }
    }

    // 计算CountMapper，统计数量
    public static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        public long count = 0;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            count += 1;
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(TEXT_COUNT, new LongWritable(count));
        }
    }

    // 计算CountReducer，统计数量
    public static class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        public long count = 0;

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            for (LongWritable v : values) {
                count += v.get();
            }
            context.write(TEXT_COUNT, new LongWritable(count));
        }
    }


    // 计算平均数MR，Map
    public static class AvgMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

        public long count = 0;
        public long sum = 0;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] v = value.toString().split("\t");
            if (v[0].equals("COUNT")) {
                count = Long.parseLong(v[1]);
            } else if (v[0].equals("SUM")) {
                sum = Long.parseLong(v[1]);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(sum), new LongWritable(count));
        }

    }

    // 计算平均数MR，Reduce
    public static class AvgReducer extends Reducer<LongWritable, LongWritable, Text, DoubleWritable> {

        public long sum = 0;
        public long count = 0;

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            sum += key.get();
            for (LongWritable v : values) {
                count += v.get();
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(TEXT_AVG, new DoubleWritable(new Double(sum) / count));
        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String inputPath = "testdata/lab4";
        String sumOutputPath = "testdata/lab4-out/sum";
        String countOutputPath = "testdata/lab4-out/count";
        String avgOutputPath = "testdata/lab4-out/avg";

        Job job1 = Job.getInstance(conf, "Sum");
        job1.setJarByClass(LinkedMR.class);
        job1.setMapperClass(SumMapper.class);
        job1.setCombinerClass(SumReducer.class);
        job1.setReducerClass(SumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(sumOutputPath));

        Job job2 = Job.getInstance(conf, "Count");
        job2.setJarByClass(LinkedMR.class);
        job2.setMapperClass(CountMapper.class);
        job2.setCombinerClass(CountReducer.class);
        job2.setReducerClass(CountReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job2, new Path(inputPath));
        FileOutputFormat.setOutputPath(job2, new Path(countOutputPath));

        Job job3 = Job.getInstance(conf, "Average");
        job3.setJarByClass(LinkedMR.class);
        job3.setMapperClass(AvgMapper.class);
        job3.setReducerClass(AvgReducer.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);

        // 将job1及job2的输出为做job3的输入
        FileInputFormat.addInputPath(job3, new Path(sumOutputPath));
        FileInputFormat.addInputPath(job3, new Path(countOutputPath));
        FileOutputFormat.setOutputPath(job3, new Path(avgOutputPath));

        // 提交job1及job2,并等待完成
        if (job1.waitForCompletion(true) && job2.waitForCompletion(true)) {
            System.exit(job3.waitForCompletion(true) ? 0 : 1);
        }
    }
}
