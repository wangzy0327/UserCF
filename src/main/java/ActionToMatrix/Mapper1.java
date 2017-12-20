package ActionToMatrix;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * key:1  2  ...
     * value:A,1,1    C,3,5   ...
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        String userId = values[0];
        String itemId = values[1];
        String score = values[2];
        outKey.set(userId);
        outValue.set(itemId+ "_" + score);
        context.write(outKey, outValue);
    }
}
