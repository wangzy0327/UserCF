package ActionToMatrix;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class Reducer1 extends Reducer<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * key:1  2...
     * value:[A_3,B_4,A_1]
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String itemId = key.toString();

        //计算用户id和打分
        Map<String, Integer> map = new HashMap<String, Integer>();

        for (Text value : values) {
            String userId = value.toString().split("_")[0];
            String score = value.toString().split("_")[1];
            if (map.get(userId) != null) {
                Integer result = Integer.valueOf(map.get(userId)) + Integer.valueOf(score);
                map.put(userId, result);
            } else {
                map.put(userId, Integer.valueOf(score));
            }
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            String userId = entry.getKey();
            Integer score = entry.getValue();
            sb.append(userId + "_" + score + ",");
        }
        String line = null;
        if (sb.toString().endsWith(",")) {
            line = sb.substring(0, sb.length() - 1);
        }
        outKey.set(itemId);
        outValue.set(line);
        context.write(outKey, outValue);
    }
}
