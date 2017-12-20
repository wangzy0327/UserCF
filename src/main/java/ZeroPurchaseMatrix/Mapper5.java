package ZeroPurchaseMatrix;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Mapper5 extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    //获取缓存中每一行数据
    private List<String> cacheList = new ArrayList<String>();

    /*** setup在map执行前最开始执行，且只执行一次
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            //通过输入流将全局缓存中的右侧矩阵读入List<String>中
            FileReader fr = new FileReader("itemUserScore3");
            BufferedReader br = new BufferedReader(fr);

            //每一行的格式是: 行 tab 列_值,列_值,列_值,列_值
            String line = null;
            while ((line = br.readLine()) != null) {
                cacheList.add(line);
            }
            fr.close();
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            //物品id:1,2,3...
            String item_matrix1 = value.toString().split("\\t")[0];
            //值:A_9.87,B_2.38,C_23.90....
            String[] user_score_array_matrix1 = value.toString().split("\\t")[1].split(",");
            for (String line : cacheList) {
                //物品id:1,2,3
                String item_matrix2 = line.split("\\t")[0];
                //值:A_2,C_5
                String[] user_score_array_matrix2 = line.split("\\t")[1].split(",");
                //如果物品id相同
                if (item_matrix1.equals(item_matrix2)) {
                    //遍历matrix1的列
                    for (String user_score_matrix1 : user_score_array_matrix1) {
                        boolean flag = false;
                        String user_matrix1 = user_score_matrix1.split("_")[0];
                        String score_matrix1 = user_score_matrix1.split("_")[1];

                        //遍历matrix2的列
                        for (String user_score_matrix2 : user_score_array_matrix2) {
                            String user_matrix2 = user_score_matrix2.split("_")[0];
                            if (user_matrix1.equals(user_matrix2)) {
                                flag = true;
                            }
                        }
                        if (!flag) {
                            outKey.set(item_matrix1);
                            outValue.set(user_matrix1 + "_" + score_matrix1);
                            context.write(outKey, outValue);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
