package ZeroPurchaseMatrix;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer5 extends Reducer<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * key:列号
     * value:[行号_值,行号_值,行号_值...]
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text value : values) {
            sb.append(value + ",");
        }
        String line = null;
        if (sb.toString().endsWith(",")) {
            line = sb.substring(0, sb.length() - 1);
        }
        outKey.set(key);
        outValue.set(line);
        System.out.println(key + " : " + line);
        context.write(outKey, outValue);
    }
}
