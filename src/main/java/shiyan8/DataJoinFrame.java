package shiyan8;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DataJoinFrame extends Configured implements Tool {

    /**
     * 继承了DataJoinMapperBase抽象类，实现了DataJion中的map功能
     *
     * 这个抽象类已经实现了map()方法
     */
    public static class MapClass extends DataJoinMapperBase {

        protected Text generateInputTag(String inputFile) {
            // inputFile为输入数据的文件名，用于提取tag
            String datasource = inputFile.split("-")[0];
            return new Text(datasource);
        }

        protected Text generateGroupKey(TaggedMapOutput aRecord) {
            String line = ((Text) aRecord.getData()).toString();
            String[] tokens = line.split(",");
            String groupKey = tokens[0];
            return new Text(groupKey);
        }

        protected TaggedMapOutput generateTaggedMapOutput(Object value) {
            TaggedWritable retv = new TaggedWritable((Text) value);
            retv.setTag(this.inputTag);
            return retv;
        }
    }

    /**
     * 继承了DataJoinReducerBase抽象类，实现了DataJion中的reduce功能
     *
     * 已经实现了reduce()方法
     */
    public static class Reduce extends DataJoinReducerBase {

        //内连接
        //第一个记录是tags[0],values[0],以此类推
        protected TaggedMapOutput combine(Object[] tags, Object[] values) {
            if (tags.length < 2)
                return null;
            String joinedStr = "";
            for (int i = 0; i < values.length; i++) {
                if (i > 0)
                    joinedStr += ",";
                TaggedWritable tw = (TaggedWritable) values[i];
                String line = ((Text) tw.getData()).toString();
                // 按“，”分割，最多分2段
                String[] tokens = line.split(",", 2);
                joinedStr += tokens[1];
            }
            TaggedWritable retv = new TaggedWritable(new Text(joinedStr));
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

        JobConf job = new JobConf(conf, DataJoinFrame.class);

        Path in = new Path("src/main/java/shiyan8/input");
        Path out = new Path("src/main/java/shiyan8/output1/" + System.currentTimeMillis());
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJobName("DataJoin");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TaggedWritable.class);
        job.set("mapred.textoutputformat.separator", ",");//tab为分别

        JobClient.runJob(job);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DataJoinFrame(), args);

        System.exit(res);
    }
}
