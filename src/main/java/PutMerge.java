import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PutMerge {

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        
        String uri = "hdfs://master:9820/usr/local/hadoop-3.1.3/tests/merge";

        Path inputDir = new Path("src/main/java/input2");
        Path hdfsFile = new Path(uri);
        try {
            FileSystem hdfs  = FileSystem.get(new URI(uri),conf);
            FileSystem local = FileSystem.getLocal(conf);
            FileStatus[] inputFiles = local.listStatus(inputDir);
            FSDataOutputStream out = hdfs.create(hdfsFile);

            for (FileStatus inputFile : inputFiles) {
                System.out.println(inputFile.getPath().getName());
                FSDataInputStream in = local.open(inputFile.getPath());
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