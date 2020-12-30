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

public class Win1 {

    public static class FirstMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private Text outputKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] stringArr = value.toString().split(","); //A,B,C
            int n = stringArr.length;
            for (int i = 0; i < n; i++) {
                for (int j = i + 1; j < n; j++) {
                    String winner = stringArr[i];
                    String loser = stringArr[j];
                    if (winner.compareTo(loser) < 0) {
                        outputKey.set(winner + "#" + loser);
                        context.write(outputKey, one);
                    } else {
                        outputKey.set(loser + "#" + winner);
                        context.write(outputKey, zero);
                    }
                }
            }
        }
    }

    public static class FirstReducer extends Reducer<Text, IntWritable, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] stringArr = key.toString().split("#");
            String candidateA = stringArr[0];
            String candidateB = stringArr[1];

            int zeroCount = 0;
            int oneCount = 0;
            for (IntWritable val : values) {
                if (val.get() == 1) {
                    oneCount++;
                } else {
                    zeroCount++;
                }
            }

            if (oneCount > zeroCount) {
                outputKey.set(candidateA);
                outputValue.set(candidateB);
                context.write(outputKey, outputValue);
            } else {
                outputKey.set(candidateB);
                outputValue.set(candidateA);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "condorcet winner1");
        job.setJarByClass(Win1.class);
        job.setMapperClass(FirstMapper.class);
        job.setReducerClass(FirstReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}