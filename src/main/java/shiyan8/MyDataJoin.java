package shiyan8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public class MyDataJoin extends Configured implements Tool {

    /**
     * 继承了DataJoinMapperBase抽象类，实现了DataJoin中的map功能
     *
     * 这个抽象类已经实现了map()方法
     */
    public static class MapClass extends DataJoinMapperBase {

        protected Text generateInputTag(String inputFile) {//1
            // inputFile为输入数据的文件名，用于提取tag
            String[] splits = inputFile.split("/");
            String fileName = splits[splits.length-1];
            return new Text(fileName);
        }

        protected Text generateGroupKey(TaggedMapOutput aRecord) {//3
            //System.out.println("inputTag is "+inputTag+" and inputFile is "+inputFile);

            String groupKey = "null";
            //inputTag is file:/C:/Users/78252/IdeaProjects/testHadoop/src/main/java/shiyan8/input/factory
            //根据不同的输入文件设置不同的GroupKey设置方法
            if(this.inputTag.toString().equals("address")){
                String line = ((Text) aRecord.getData()).toString();
                String[] tokens = line.split(" ");//以空格分开，第一个即为groupKey
                groupKey = tokens[0];
            }
            if(this.inputTag.toString().equals("factory")){
                String line = ((Text) aRecord.getData()).toString();
                String[] tokens = line.split(" ");//以空格分开，最后一个即为groupKey
                groupKey = tokens[tokens.length-1];
            }
            //将groupKey为addressID的目录的key设为ASCII码更为前面的字符，因为map按key值重分区给reduce，则其会出现在第一行
            if(groupKey.equals("addressID")){
                groupKey = "0";
            }

            return new Text(groupKey);
        }

        protected TaggedMapOutput generateTaggedMapOutput(Object value) {//2
            TaggedWritable retv = new TaggedWritable((Text) value);//即一行的全部内容
            retv.setTag(this.inputTag);
            return retv;
        }
    }

    /**
     * 继承了DataJoinReducerBase抽象类，实现了DataJoin中的reduce功能
     *
     * 已经实现了reduce()方法
     */
    public static class Reduce extends DataJoinReducerBase {

        @Override
        //将key重置为空，使得输出时不会第一列输出groupKey
        //看源码可知，调用该reduce前，已经将其根据不同的key分好了，regroup已将同一个key放在同一个<TreeMap>Group
        public void reduce(Object key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {
            key = new Text();
            super.reduce(key, values, output, reporter);
        }

        //内连接
        //第一个记录是tags[0],values[0],以此类推
        protected TaggedMapOutput combine(Object[] tags, Object[] values) {
            if (tags.length < 2){
               /* String joinedStr = "";
                TaggedWritable tw = (TaggedWritable) values[0];
                String line = ((Text) tw.getData()).toString();
                String[] tokens = line.split(" ");
                if(tags[0].toString().equals("address")){
                    joinedStr += tokens[tokens.length-1];
                    joinedStr += "\t";
                    joinedStr += tokens[0];
                }else {
                    for (int j = 0; j < tokens.length-1; j++) {
                        joinedStr += tokens[j];
                        joinedStr += " ";
                    }
                    joinedStr += "\t";
                    joinedStr += tokens[tokens.length-1];
                }
                TaggedWritable retv = new TaggedWritable(new Text(joinedStr));
                retv.setTag((Text) tags[0]);
                return retv;*/
               return null;
            }

            String joinedStr = "";

            for (int i = values.length-1; i >=0 ; i--) {
                TaggedWritable tw = (TaggedWritable) values[i];
                String line = ((Text) tw.getData()).toString();
                String[] tokens = line.split(" ");
                if(i >= 1) {
                    for (int j = 0; j < tokens.length-1; j++) {
                        joinedStr += tokens[j];
                        joinedStr += " ";
                    }
                    joinedStr += "\t";
                    joinedStr += tokens[tokens.length-1];
                }
                if (i == 0){
                    joinedStr += "\t";
                    joinedStr += tokens[1];
                }

            }
            TaggedWritable retv = new TaggedWritable(new Text(joinedStr));
            //System.out.println(tags[0]+" and "+tags[1]); result:address and factory
            retv.setTag((Text) tags[0]);
            return retv;
        }


    }

    /**
     * 继承TaggedMapOutput抽象类，定义了在datajoin中的value类型
     */
    public static class TaggedWritable extends TaggedMapOutput {

        private Text data;

        /**
         * 初始化
         */
        public TaggedWritable() {
            this.data = new Text();
        }

        public TaggedWritable(Writable data) {
            this.tag = new Text("");
            this.data = (Text) data;
        }

        public Writable getData() {
            return data;
        }

        public void write(DataOutput out) throws IOException {
            this.tag.write(out);
            this.data.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            this.tag.readFields(in);
            this.data.readFields(in);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        /*conf.set("mapred.textoutputformat.ignoreseparator","true");
        conf.set("mapred.textoutputformat.separator","\0");*/

        JobConf job = new JobConf(conf, MyDataJoin.class);

        Path in = new Path("src/main/java/shiyan8/input");
        Path out = new Path("src/main/java/shiyan8/output1/" + System.currentTimeMillis());
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJobName("DataJoin");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TaggedWritable.class);
        job.set("mapred.textoutputformat.separator", " ");//tab为分别
        JobClient.runJob(job);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MyDataJoin(), args);
        Runtime run = Runtime.getRuntime();
        long total = run.totalMemory();
        long free = run.freeMemory();
        System.out.println("已使用内存 = " + (total - free));
        System.exit(res);
    }
}
