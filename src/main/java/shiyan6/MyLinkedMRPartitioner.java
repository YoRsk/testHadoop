package shiyan6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyLinkedMRPartitioner {
    // Mapper
    public static class MyMapper extends Mapper<LongWritable, Text, Text, NumWritable> {
        public NumWritable numWritable = new NumWritable();
        public long sum = 0;
        public long max = 0;
        public long min = 0;
        public long count = 0;
        public double avg = 0;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit file = (FileSplit)context.getInputSplit();
            String fileName = file.getPath().getName();//读取的文件名

            sum = numWritable.Sum(Long.parseLong(value.toString()));
            max = numWritable.Max(Long.parseLong(value.toString()));
            min = numWritable.Min(Long.parseLong(value.toString()));
            count = numWritable.Count();
            context.write(new Text(fileName), numWritable);
        }
      /*  protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(fileName), numWritable);
        }*/
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
              /*  System.out.println(v.getSum());
                System.out.println(v.getMax());
                System.out.println(v.getMin());
                System.out.println(v.getCount());*/
                sum = numWritable.Sum(v.getSum());
                max = numWritable.Max(v.getMax());
                min = numWritable.Min(v.getMin());
                count += v.getCount();
            }
            numWritable.setCount(count);
            avg = numWritable.Avg();
            String text =
                    key+"-sum " + sum + "\n" +
                    key+"-max " + max + "\n" +
                    key+"-min " + min + "\n" +
                    key+"-avg " + avg ;//key为文件名
            context.write(NullWritable.get(), new Text(text));
        }
    }
    public static class MyPartitioner
            extends Partitioner<Text, NumWritable> {
        @Override
        public int getPartition(Text key, NumWritable value,
                                int numPartitions) {
            int number=(int)key.toString().charAt(key.getLength()-1);//获取文件名最后一位上的编号
            System.out.println("number is "+number);
            return number%numPartitions;//根据编号划分

        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");

        String inputPath = "src/main/java/shiyan6/Input";
        String myOutputPath = "src/main/java/shiyan6/Output/results2";
        Job job = Job.getInstance(conf,"results");
        job.setJarByClass(MyLinkedMR.class);
        job.setMapperClass(MyMapper.class);
        //job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NumWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(3);//设置三个
        job.setPartitionerClass(MyPartitioner.class);
        FileInputFormat.addInputPath(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(myOutputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
