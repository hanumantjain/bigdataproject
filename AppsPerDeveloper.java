package org.words;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AppsPerDeveloper {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private Text developerId = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("App_Id")) { // Skip header line
                return;
            }
            String[] parts = line.split(",");
            if (parts.length > 12) { // Ensure developer ID field is present
                String devId = parts[12];
                if (!devId.isEmpty()) {
                    developerId.set(devId);
                    context.write(developerId, one); // Emit developer ID with count 1
                }
            }
        }
    }


    public static class RatingStatsReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result); // Emit developer ID along with total count
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "developer rating stats");
        job.setJarByClass(AppsPerDeveloper.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(RatingStatsReducer.class);
        job.setReducerClass(RatingStatsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
