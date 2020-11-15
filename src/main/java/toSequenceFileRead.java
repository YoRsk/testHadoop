import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class toSequenceFileRead {
     public static final  String uri = "src/main/java/sequenceFiles/sequenceFile";

    public static void main(String[] args) throws IOException {
         Configuration conf = new Configuration();
        LocalFileSystem fs = FileSystem.getLocal(conf);
        Path path = new Path(uri);
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();

            //三种查询方法如下：
            //getFile(reader,"file1",conf);
            //getKey(reader,3130,conf);
            getFileAndKey(reader,3130,"file5",conf);


        } finally {
            IOUtils.closeStream(reader);
        }
    }

//    3.1）给出文件名，可以从序列文件整体读取文件并存储到指定的位置；
    public static void getFile(SequenceFile.Reader reader, String file, Configuration conf) throws IOException {
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        long position = reader.getPosition();
        while (reader.next(key, value)) {
            String [] filePath=key.toString().split("/");

            if(filePath[filePath.length-1].equals(file)){
                byte[] bytes = value.copyBytes();
                String content = new String(bytes);
                String[] ans = content.split("[\t\n]");
                for(int i=0;i<200;i+=2){
                    System.out.println("key and value is "+ans[i]+"\t"+ans[i+1]);
                }
            }

        }
    }

// 3.2）给出某个整数的key，可以读取所有该key的数据，并给出所在文件的名称（可以输出到控制台）
    public static void getKey(SequenceFile.Reader reader, Integer key1, Configuration conf) throws IOException {
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        //newInstance: Create an object for the given class and initialize it from conf
        //所以key其实在conf中，reader.geyKeyClass只是获取key 的class
        BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        long position = reader.getPosition();
        while (reader.next(key, value)) {
            String [] filePath=key.toString().split("/");

                byte[] bytes = value.copyBytes();
                String content = new String(bytes);
                String[] ans = content.split("[\t\n]");
                for(int i=0;i<ans.length;i+=2){
                    if(ans[i].equals(key1.toString())){
                        System.out.println("file is "+filePath[filePath.length-1]);
                        System.out.println("key and value is "+ans[i]+"\t"+ans[i+1]);
                    }
                }


        }
    }
//    3.3）给出文件名和整数的key，可以读取该文件中的对应key的数据（可以输出到控制台）
    public static void getFileAndKey(SequenceFile.Reader reader, Integer key1, String file, Configuration conf) throws IOException {
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        long position = reader.getPosition();
        while (reader.next(key, value)) {
            String [] filePath=key.toString().split("/");//数组中的最后一个即为文件名
            if(filePath[filePath.length-1].equals(file)){
                byte[] bytes = value.copyBytes();
                String content = new String(bytes);
                String[] ans = content.split("[\t\n]");//得到每一行
                for(int i=0;i<ans.length;i+=2){
                    if(ans[i].equals(key1.toString())){
                        System.out.println("file is "+filePath[filePath.length-1]);
                        System.out.println("key and value is "+ans[i]+"\t"+ans[i+1]);
                    }
                }
            }
        }
    }


}
