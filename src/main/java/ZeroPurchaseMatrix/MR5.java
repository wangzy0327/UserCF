package ZeroPurchaseMatrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MR5 {
    //输入文件路径
    //private static String inPath = "/user/wzy/UserCF/step4_output";
    private static String inPath = "UserCF/step4_output";
    //输出文件路径
    //private static String outPath = "/user/wzy/ItemCF/step5_output";
    private static String outPath = "UserCF/step5_output";
    //设置缓存路径
    //这样让路径直接找输出矩阵所在的文件，它的别名才起作用
    //private static String cache = "/user/wzy/ItemCF/step1_output/part-r-00000";
    private static String cache = "UserCF/step1_output/part-r-00000";
    //hdfs文件地址
    //注意要同时修改core-site.xml文件里面的fs.defaultFS(hdfs://localhost:9000) 为hdfs://master:9000
    //private static String hdfs = "hdfs://localhost:9000";
    private static String hdfs = "hdfs://master:9000";

    public static int run() {
        try {
            //创建job配置类
            Configuration conf = new Configuration();
            //设置hdfs地址
            conf.set("fs.defaultFS", hdfs);
            //创建一个job实例
            Job job = Job.getInstance(conf, "step5");

            System.out.println(cache + "#itemUserScore3");
            //添加分布式缓存文件
            job.addCacheArchive(new URI(cache + "#itemUserScore3"));

            //设置job的主类
            job.setJarByClass(MR5.class);
            //设置job的Mapper和Reduce
            job.setMapperClass(Mapper5.class);
            job.setReducerClass(Reducer5.class);
            //设置Mapper的输出key和value类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            //设置Reducer的输出key和value类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileSystem fs = FileSystem.get(conf);
            Path inputPath = new Path(inPath);
            //设置输入输出路径
            if (fs.exists(inputPath)) {
                FileInputFormat.addInputPath(job, inputPath);
            }
            Path outputPath = new Path(outPath);
            //如果输出路径存在则删除路径
            fs.delete(outputPath, true);
            //将输出路径添加到job中
            FileOutputFormat.setOutputPath(job, outputPath);

            return job.waitForCompletion(true) ? 1 : -1;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args) {
        int result = 0;
        //注意运行写的类,之前出错是因为写的是MR4..........
        result = new MR5().run();
        if (result == 1) {
            System.out.println("step5运行成功!");
        } else {
            System.out.println("step5运行失败!");
        }
    }
    /**
     * File System Counters
     FILE: Number of bytes read=786
     FILE: Number of bytes written=671402
     FILE: Number of read operations=0
     FILE: Number of large read operations=0
     FILE: Number of write operations=0
     HDFS: Number of bytes read=484
     HDFS: Number of bytes written=130
     HDFS: Number of read operations=45
     HDFS: Number of large read operations=0
     HDFS: Number of write operations=6
     Map-Reduce Framework
     Map input records=6
     Map output records=16
     Map output bytes=150
     Map output materialized bytes=188
     Input split bytes=128
     Combine input records=0
     Combine output records=0
     Reduce input groups=6
     Reduce shuffle bytes=188
     Reduce input records=16
     Reduce output records=6
     Spilled Records=32
     Shuffled Maps =1
     Failed Shuffles=0
     Merged Map outputs=1
     GC time elapsed (ms)=0
     Total committed heap usage (bytes)=634388480
     Shuffle Errors
     BAD_ID=0
     CONNECTION=0
     IO_ERROR=0
     WRONG_LENGTH=0
     WRONG_MAP=0
     WRONG_REDUCE=0
     File Input Format Counters
     Bytes Read=194
     File Output Format Counters
     Bytes Written=130
     */
}
