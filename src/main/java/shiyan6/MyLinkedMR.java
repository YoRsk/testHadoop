package shiyan6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyLinkedMR {
    private static final Text TEXT_NUMWRITABLE = new Text("NUMWRITABLE");

    // Mapper
    public static class MyMapper extends Mapper<LongWritable, Text, Text, NumWritable> {
        public NumWritable numWritable = new NumWritable();
        public long sum = 0;
        public long max = 0;
        public long min = 0;
        public long count = 0;
        public double avg = 0;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            sum = numWritable.Sum(Long.parseLong(value.toString()));
            max = numWritable.Max(Long.parseLong(value.toString()));
            min = numWritable.Min(Long.parseLong(value.toString()));
            count = numWritable.Count();
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(TEXT_NUMWRITABLE, numWritable);
        }
    }

    //Reduce
    public static class MyReducer extends Reducer<Text, NumWritable, NullWritable, Text> {
        public NumWritable numWritable = new NumWritable();
        public long sum = 0;
        public long max = 0;
        public long min = 0;
        public long count = 0;
        public double avg = 0;

        public void reduce(Text key, Iterable<NumWritable> values, Context context)
                throws IOException, InterruptedException {
            for (NumWritable v : values) {
                System.out.println(v.getSum());
                System.out.println(v.getMax());
                System.out.println(v.getMin());
                System.out.println(v.getCount());
                sum = numWritable.Sum(v.getSum());
                max = numWritable.Max(v.getMax());
                min = numWritable.Min(v.getMin());
                count += v.getCount();
            }
            numWritable.setCount(count);
            avg = numWritable.Avg();
            context.write(NullWritable.get(), new Text(numWritable.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");

        String inputPath = "src/main/java/shiyan6/Input";
        String myOutputPath = "src/main/java/shiyan6/Output/results";
        Job job = Job.getInstance(conf,"results");
        job.setJarByClass(MyLinkedMR.class);
        job.setMapperClass(MyMapper.class);
        //job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NumWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(myOutputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
/*
        Job job1 = Job.getInstance(conf, "Sum");
        job1.setJarByClass(MyLinkedMR.class);
        job1.setMapperClass(SumMapper.class);
        job1.setCombinerClass(SumReducer.class);
        job1.setReducerClass(SumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(sumOutputPath));

        Job job2 = Job.getInstance(conf, "Count");
        job2.setJarByClass(MyLinkedMR.class);
        job2.setMapperClass(CountMapper.class);
        job2.setCombinerClass(CountReducer.class);
        job2.setReducerClass(CountReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job2, new Path(inputPath));
        FileOutputFormat.setOutputPath(job2, new Path(countOutputPath));

        Job job3 = Job.getInstance(conf, "Average");
        job3.setJarByClass(MyLinkedMR.class);
        job3.setMapperClass(AvgMapper.class);
        job3.setReducerClass(AvgReducer.class);
        *//*当mapper与reducer的输出类型一致时可以用　job.setOutputKeyClass(theClass)与job.setOutputValueClass
        (theClass)这两个进行配置就行，但是当mapper用于reducer两个的输出类型不一致的时候就需
        要分别进行配置了。
         *//*
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);*/


/*
        // 将job1及job2的输出为做job3的输入
        FileInputFormat.addInputPath(job3, new Path(sumOutputPath));
        FileInputFormat.addInputPath(job3, new Path(countOutputPath));
        FileOutputFormat.setOutputPath(job3, new Path(avgOutputPath));

        // 提交job1及job2,并等待完成
        if (job1.waitForCompletion(true) && job2.waitForCompletion(true)) {
            System.exit(job3.waitForCompletion(true) ? 0 : 1);
        }*/
    }
}
