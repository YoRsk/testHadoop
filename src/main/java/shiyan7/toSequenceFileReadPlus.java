package shiyan7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import shiyan6.NumWritable;

import java.io.*;
import java.text.NumberFormat;
import java.util.*;

public class toSequenceFileReadPlus {
    public static final  String uri = "src/main/java/sequenceFilesPlus/sequenceFilePlus";
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

            //三种方法如下：
            //1 读取单个文件中的key（可以是从1到100000的随机整数），统计“单个”文件中数据按位分布
            getFile(reader,"file2",conf);
            //修改上述程序1，输出对所有文件执行程序1汇总后，找到分布中Top10的数据分布以及所在文件信息；
            getTop10Files(reader,conf);
            //修改上述程序1，不区分文件的情况下进行统计所有文件中以上的数据按位分布情况。
            getAllFiles(reader,conf);
        } finally {
            IOUtils.closeStream(reader);
        }
    }
   //1、读取单个文件中的key（可以是从1到100000的随机整数），统计“单个”文件中数据按位分布：
    public static void getFile(SequenceFile.Reader reader,String file, Configuration conf) throws IOException {
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        long position = reader.getPosition();
        int sum = 0;//总的key
        int units = 0,tens = 0,hundreds = 0,thousands = 0,tenThousands = 0, hundredThousands = 0;

        while (reader.next(key, value)) {
            String [] filePath=key.toString().split("/");

            if(filePath[filePath.length-1].equals(file)){
                byte[] bytes = value.copyBytes();
                String content = new String(bytes);
                String[] ans = content.split("[\t\n]");
                for(int i=0;i<200000;i+=2){
                    int num = Integer.parseInt(ans[i]);
                    sum++;
                    if(1<=num&&num<=9){
                        units++;
                    }else if(10<=num&&num<=99){
                        tens++;
                    }else if(100<=num&&num<=999){
                        hundreds++;
                    }else if(1000<=num&&num<=9999){
                        thousands++;
                    }else if(10000<=num&&num<=99999){
                        tenThousands++;
                    }else hundredThousands++;
                }
            }
        }
        //输出路径及百分比数字格式化
        String url = "src/main/java/shiyan7/1/out";
        FileOutputStream fOut = null;
        NumberFormat nf = NumberFormat.getPercentInstance();
        nf.setMinimumFractionDigits(3);//3位小数
        try{
            fOut = new FileOutputStream(new File(url));
            PrintStream printStream = new PrintStream(new BufferedOutputStream(fOut));
            printStream.print(file+": ");
            printStream.print("个位数："+nf.format((double)units/sum)+"  ");
            printStream.print("十位数："+nf.format((double)tens/sum)+"  ");
            printStream.print("百位数："+nf.format((double)hundreds/sum)+"  ");
            printStream.print("千位数："+nf.format((double)thousands/sum)+"  ");
            printStream.print("万位数："+nf.format((double)tenThousands/sum)+"  ");
            printStream.print("十万位数："+nf.format((double)hundredThousands/sum)+"");
            printStream.close();
            fOut.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    //2、修改上述程序1，输出对所有文件执行程序1汇总后，找到分布中Top10的数据分布以及所在文件信息；
    //getFileInfor获取FileInfor数组，每个FileInfor对象包含一个文件的编号、个位至十万位 每位的个数
    //FileInforSort根据不同flag，决定个位、十位、百位而数组进行排序，并输出至输出文件。例如flag == 6 则List最后十个即为个位的top10
    //getTop10Files 生成不同的flag，运行多次FileInforSort函数
    public static List<FileInfor> getFileInfor(SequenceFile.Reader reader,Configuration conf) throws IOException {
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        long position = reader.getPosition();
        List<FileInfor>fList = new ArrayList<>();//存储每一个FileInfor对象

        while (reader.next(key, value)) {
            String [] filePath=key.toString().split("/");
            String [] No = filePath[filePath.length-1].split("\\D");//取出路径中最后一段数字即为文件的编号

            FileInfor fileInfor = new FileInfor(Integer.parseInt(No[No.length-1]));//生成FileInfor对象
            byte[] bytes = value.copyBytes();
            String content = new String(bytes);
            String[] ans = content.split("[\t\n]");
            for(int i=0;i<200000;i+=2){
                int num = Integer.parseInt(ans[i]);
                fileInfor.sum++;
                if(1<=num&&num<=9){
                    fileInfor.units++;
                }else if(10<=num&&num<=99){
                    fileInfor.tens++;
                }else if(100<=num&&num<=999){
                    fileInfor.hundreds++;
                }else if(1000<=num&&num<=9999){
                    fileInfor.thousands++;
                }else if(10000<=num&&num<=99999){
                    fileInfor.tenThousands++;
                }else fileInfor.hundredThousands++;
            }
            fList.add(fileInfor);//将每一个文件的编号，个位数个数至 十万位数个数保存到一个对象后，加入到该List中
        }
        return fList;
    }
    public static void FileInforSort(List<FileInfor>fList,int flag) {
        //输出路径及百分比数字格式化
        String url = "src/main/java/shiyan7/2/out";
        FileOutputStream fOut = null;
        NumberFormat nf = NumberFormat.getPercentInstance();
        nf.setMinimumFractionDigits(3);//三位小数

        if (flag == 6) {//flag == 6时 将fileInfors 对于个位数排序
            fList.sort(new Comparator<FileInfor>() {
                @Override
                public int compare(FileInfor o1, FileInfor o2) {
                    if (o1.units > o2.units) return 1;
                    else if (o1.units == o2.units) return 0;
                    else return -1;
                }
            });
            try {
                fOut = new FileOutputStream(url);
                PrintStream printStream = new PrintStream(new BufferedOutputStream(fOut), true);
                printStream.print("个位数的top10：");
                for (int i = 0; i < 10; i++) {
                    FileInfor fileInfor = fList.get(fList.size() - 1 - i);
                    printStream.print("file" + fileInfor.no + " ");
                }
                printStream.println();
                for (int i = 0; i < 10; i++) {
                    FileInfor fileInfor = fList.get(fList.size() - 1 - i);
                    printStream.print("file" + fileInfor.no + " ");
                    printStream.print("个位数：" + nf.format((double) fileInfor.units / fileInfor.sum) + "  ");
                    printStream.print("十位数：" + nf.format((double) fileInfor.tens / fileInfor.sum) + "  ");
                    printStream.print("百位数：" + nf.format((double) fileInfor.hundreds / fileInfor.sum) + "  ");
                    printStream.print("千位数：" + nf.format((double) fileInfor.thousands / fileInfor.sum) + "  ");
                    printStream.print("万位数：" + nf.format((double) fileInfor.tenThousands / fileInfor.sum) + "  ");
                    printStream.println("十万位数：" + nf.format((double) fileInfor.hundredThousands / fileInfor.sum) + "");
                }
                printStream.println();
                printStream.close();
                fOut.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (flag == 5) {//flag == 5时 将fileInfors 对于十位数排序
            fList.sort(new Comparator<FileInfor>() {
                @Override
                public int compare(FileInfor o1, FileInfor o2) {
                    if (o1.tens > o2.tens) return 1;
                    else if (o1.tens == o2.tens) return 0;
                    else return -1;
                }
            });
            try {
                fOut = new FileOutputStream(url,true);
                PrintStream printStream = new PrintStream(new BufferedOutputStream(fOut), true);
                printStream.print("十位数的top10：");
                for (int i = 0; i < 10; i++) {
                    FileInfor fileInfor = fList.get(fList.size() - 1 - i);
                    printStream.print("file" + fileInfor.no + " ");
                }
                printStream.println();
                for (int i = 0; i < 10; i++) {
                    FileInfor fileInfor = fList.get(fList.size() - 1 - i);
                    printStream.print("file" + fileInfor.no + " ");
                    printStream.print("个位数：" + nf.format((double) fileInfor.units / fileInfor.sum) + "  ");
                    printStream.print("十位数：" + nf.format((double) fileInfor.tens / fileInfor.sum) + "  ");
                    printStream.print("百位数：" + nf.format((double) fileInfor.hundreds / fileInfor.sum) + "  ");
                    printStream.print("千位数：" + nf.format((double) fileInfor.thousands / fileInfor.sum) + "  ");
                    printStream.print("万位数：" + nf.format((double) fileInfor.tenThousands / fileInfor.sum) + "  ");
                    printStream.println("十万位数：" + nf.format((double) fileInfor.hundredThousands / fileInfor.sum) + "");
                }
                printStream.println();
                printStream.close();
                fOut.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (flag == 4) {//flag == 4时 将fileInfors 对于百位数排序
            fList.sort(new Comparator<FileInfor>() {
                @Override
                public int compare(FileInfor o1, FileInfor o2) {
                    if (o1.hundreds > o2.hundreds) return 1;
                    else if (o1.hundreds == o2.hundreds) return 0;
                    else return -1;
                }
            });
            try {
                fOut = new FileOutputStream(url,true);
                PrintStream printStream = new PrintStream(new BufferedOutputStream(fOut), true);
                printStream.print("百位数的top10：");
                for (int i = 0; i < 10; i++) {
                    FileInfor fileInfor = fList.get(fList.size() - 1 - i);
                    printStream.print("file" + fileInfor.no + " ");
                }
                printStream.println();
                for (int i = 0; i < 10; i++) {
                    FileInfor fileInfor = fList.get(fList.size() - 1 - i);
                    printStream.print("file" + fileInfor.no + " ");
                    printStream.print("个位数：" + nf.format((double) fileInfor.units / fileInfor.sum) + "  ");
                    printStream.print("十位数：" + nf.format((double) fileInfor.tens / fileInfor.sum) + "  ");
                    printStream.print("百位数：" + nf.format((double) fileInfor.hundreds / fileInfor.sum) + "  ");
                    printStream.print("千位数：" + nf.format((double) fileInfor.thousands / fileInfor.sum) + "  ");
                    printStream.print("万位数：" + nf.format((double) fileInfor.tenThousands / fileInfor.sum) + "  ");
                    printStream.println("十万位数：" + nf.format((double) fileInfor.hundredThousands / fileInfor.sum) + "");
                }
                printStream.println();
                printStream.close();
                fOut.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (flag == 3) {//flag == 3时 将fileInfors 对于千位数排序
            fList.sort(new Comparator<FileInfor>() {
                @Override
                public int compare(FileInfor o1, FileInfor o2) {
                    if (o1.thousands > o2.thousands) return 1;
                    else if (o1.thousands == o2.thousands) return 0;
                    else return -1;
                }
            });
            try {
                fOut = new FileOutputStream(url,true);
                PrintStream printStream = new PrintStream(new BufferedOutputStream(fOut), true);
                printStream.print("千位数的top10：");
                for (int i = 0; i < 10; i++) {
                    FileInfor fileInfor = fList.get(fList.size() - 1 - i);
                    printStream.print("file" + fileInfor.no + " ");
                }
                printStream.println();
                for (int i = 0; i < 10; i++) {
                    FileInfor fileInfor = fList.get(fList.size() - 1 - i);
                    printStream.print("file" + fileInfor.no + " ");
                    printStream.print("个位数：" + nf.format((double) fileInfor.units / fileInfor.sum) + "  ");
                    printStream.print("十位数：" + nf.format((double) fileInfor.tens / fileInfor.sum) + "  ");
                    printStream.print("百位数：" + nf.format((double) fileInfor.hundreds / fileInfor.sum) + "  ");
                    printStream.print("千位数：" + nf.format((double) fileInfor.thousands / fileInfor.sum) + "  ");
                    printStream.print("万位数：" + nf.format((double) fileInfor.tenThousands / fileInfor.sum) + "  ");
                    printStream.println("十万位数：" + nf.format((double) fileInfor.hundredThousands / fileInfor.sum) + "");
                }
                printStream.println();
                printStream.close();
                fOut.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (flag == 2) {//flag == 2时 将fileInfors 对于万位数排序
            fList.sort(new Comparator<FileInfor>() {
                @Override
                public int compare(FileInfor o1, FileInfor o2) {
                    if (o1.tenThousands > o2.tenThousands) return 1;
                    else if (o1.tenThousands == o2.tenThousands) return 0;
                    else return -1;
                }
            });
            try {
                fOut = new FileOutputStream(url,true);
                PrintStream printStream = new PrintStream(new BufferedOutputStream(fOut), true);
                printStream.print("万位数的top10：");
                for (int i = 0; i < 10; i++) {
                    FileInfor fileInfor = fList.get(fList.size() - 1 - i);
                    printStream.print("file" + fileInfor.no + " ");
                }
                printStream.println();
                for (int i = 0; i < 10; i++) {
                    FileInfor fileInfor = fList.get(fList.size() - 1 - i);
                    printStream.print("file" + fileInfor.no + " ");
                    printStream.print("个位数：" + nf.format((double) fileInfor.units / fileInfor.sum) + "  ");
                    printStream.print("十位数：" + nf.format((double) fileInfor.tens / fileInfor.sum) + "  ");
                    printStream.print("百位数：" + nf.format((double) fileInfor.hundreds / fileInfor.sum) + "  ");
                    printStream.print("千位数：" + nf.format((double) fileInfor.thousands / fileInfor.sum) + "  ");
                    printStream.print("万位数：" + nf.format((double) fileInfor.tenThousands / fileInfor.sum) + "  ");
                    printStream.println("十万位数：" + nf.format((double) fileInfor.hundredThousands / fileInfor.sum) + "");
                }
                printStream.println();
                printStream.close();
                fOut.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (flag == 1) {//flag == 1时 将fileInfors 对于十万位数排序
            fList.sort(new Comparator<FileInfor>() {
                @Override
                public int compare(FileInfor o1, FileInfor o2) {
                    if (o1.hundredThousands > o2.hundredThousands) return 1;
                    else if (o1.hundredThousands == o2.hundredThousands) return 0;
                    else return -1;
                }
            });
            try {
                fOut = new FileOutputStream(url,true);
                PrintStream printStream = new PrintStream(new BufferedOutputStream(fOut), true);
                printStream.print("十万位数的top10：");
                for (int i = 0; i < 10; i++) {
                    FileInfor fileInfor = fList.get(fList.size() - 1 - i);
                    printStream.print("file" + fileInfor.no + " ");
                }
                printStream.println();
                for (int i = 0; i < 10; i++) {
                    FileInfor fileInfor = fList.get(fList.size() - 1 - i);
                    printStream.print("file" + fileInfor.no + " ");
                    printStream.print("个位数：" + nf.format((double) fileInfor.units / fileInfor.sum) + "  ");
                    printStream.print("十位数：" + nf.format((double) fileInfor.tens / fileInfor.sum) + "  ");
                    printStream.print("百位数：" + nf.format((double) fileInfor.hundreds / fileInfor.sum) + "  ");
                    printStream.print("千位数：" + nf.format((double) fileInfor.thousands / fileInfor.sum) + "  ");
                    printStream.print("万位数：" + nf.format((double) fileInfor.tenThousands / fileInfor.sum) + "  ");
                    printStream.println("十万位数：" + nf.format((double) fileInfor.hundredThousands / fileInfor.sum) + "");
                }
                printStream.println();
                printStream.close();
                fOut.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public static void getTop10Files(SequenceFile.Reader reader,Configuration conf)throws IOException{
        List<FileInfor> fList = getFileInfor(reader, conf);//获取所有文件的信息
        int flag = 6;
        while(flag >= 1){
            FileInforSort(fList,flag);//根据某位数将数组排好序，然后取数组的最后十个即为该位数上top10
            flag--;
        }
    }

    //3修改上述程序1，不区分文件的情况下进行统计所有文件中以上的数据按位分布情况。
    public static void getAllFiles(SequenceFile.Reader reader,Configuration conf)throws IOException{
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        long position = reader.getPosition();
        int sum = 0;//总的key
        int units = 0,tens = 0,hundreds = 0,thousands = 0,tenThousands = 0, hundredThousands = 0;

        while (reader.next(key, value)) {
            byte[] bytes = value.copyBytes();
            String content = new String(bytes);
            String[] ans = content.split("[\t\n]");
            for(int i=0;i<200000;i+=2){
                int num = Integer.parseInt(ans[i]);
                sum++;
                if(1<=num&&num<=9){
                    units++;
                }else if(10<=num&&num<=99){
                    tens++;
                }else if(100<=num&&num<=999){
                    hundreds++;
                }else if(1000<=num&&num<=9999){
                    thousands++;
                }else if(10000<=num&&num<=99999){
                    tenThousands++;
                }else hundredThousands++;
            }

        }
        String url = "src/main/java/shiyan7/3/out";
        FileOutputStream fOut = null;
        NumberFormat nf = NumberFormat.getPercentInstance();
        nf.setMinimumFractionDigits(3);//三位小数
        try{
            fOut = new FileOutputStream(new File(url));
            PrintStream printStream = new PrintStream(new BufferedOutputStream(fOut));
            printStream.print("All files: ");
            printStream.print("个位数："+nf.format((double)units/sum)+"  ");
            printStream.print("十位数："+nf.format((double)tens/sum)+"  ");
            printStream.print("百位数："+nf.format((double)hundreds/sum)+"  ");
            printStream.print("千位数："+nf.format((double)thousands/sum)+"  ");
            printStream.print("万位数："+nf.format((double)tenThousands/sum)+"  ");
            printStream.print("十万位数："+nf.format((double)hundredThousands/sum)+"");
            printStream.close();
            fOut.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }


}
