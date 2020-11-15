package Codes;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountWithPartitioner {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  
  public static class LetterPartitioner 
  extends  Partitioner<Text, IntWritable>{
	@Override
	public int getPartition(Text key, IntWritable value, 
			int numPartitions) {
		int letterLocation=key.toString().toUpperCase().charAt(0)-'A'+1;
		int[] interval=new int[numPartitions];
		for (int j = 0; j < interval.length; j++) interval[j]=26/numPartitions;
		for (int i=0;i<26%numPartitions;i++) interval[i]++;
		for(int i=0;i<numPartitions;i++) {
			if (letterLocation<=interval[i])return i;
		}
		return 0;
	}
	  
  }

  @SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = {"testdata/input3","testdata/output0508/4"};   
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCountWithPartitioner.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setPartitionerClass(LetterPartitioner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.setNumReduceTasks(2);
    
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
