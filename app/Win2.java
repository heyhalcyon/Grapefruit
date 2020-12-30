
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.HashMap;

public class Win2 {
    public static class SecondMapper extends Mapper<Object, Text, IntWritable, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text outputValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] elems = value.toString().split("\\s+");
            outputValue.set(elems[0] + "#" + elems[1]);
            context.write(one, outputValue);
        }
    }

    public static class SecondReducer extends Reducer<IntWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> voteMap = new HashMap<>();

            for (Text val : values) {
                String[] candidates = val.toString().split("#");
                voteMap.put(candidates[0], voteMap.getOrDefault(candidates[0], 0) + 1);
            }

            int maxCount = -1;

            for (String candidate: voteMap.keySet()) {
                if (voteMap.get(candidate) == 2) {
                    outputKey.set(candidate);
                    outputValue.set("Condorcet winner!");
                    context.write(outputKey, outputValue);
                    return;
                }
                if (voteMap.get(candidate) >= maxCount) {
                    maxCount = voteMap.get(candidate);
                }
            }

            String result = "";
            for (String candidate: voteMap.keySet()) {
                if (voteMap.get(candidate) == maxCount) {
                    result += candidate + "#";
                }
            }

            outputKey.set(result.substring(0, result.length()-1));
            outputValue.set("No Condorcet winner, Highest Condorcet counts");
            context.write(outputKey, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "condorcet winner2");
        job.setJarByClass(Win2.class);
        job.setMapperClass(SecondMapper.class);
        job.setReducerClass(SecondReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
