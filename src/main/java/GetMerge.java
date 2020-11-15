import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

public class GetMerge {

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();

        String uri = "hdfs://master:9820/usr/local/hadoop-3.1.3/tests/merge";

        Path inputDir = new Path("src/main/java/Merger.gz");//一定要声明文件的类型.txt，不然会拒绝访问
        Path hdfsFile = new Path(uri);
        try {
            FileSystem hdfs  = FileSystem.get(new URI(uri),conf);
            FileSystem local = FileSystem.getLocal(conf);
            FileStatus[] inputFiles = hdfs.listStatus(hdfsFile);
            // 创建一个编码解码器，通过反射机制根据传入的类名来动态生成其实例
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName("org.apache.hadoop.io.compress.GzipCodec"), conf);
            OutputStream out = codec.createOutputStream(local.create(inputDir));

            for (FileStatus inputFile : inputFiles) {
                System.out.println(inputFile.getPath().getName());
                FSDataInputStream in = hdfs.open(inputFile.getPath());
                byte[] buffer = new byte[256];
                int bytesRead = 0;
                while ((bytesRead = in.read(buffer)) > 0) {
                    out.write(buffer, 0, bytesRead);
                }
                in.close();
            }
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}