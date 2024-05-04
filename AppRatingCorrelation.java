package org.words;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AppRatingCorrelation {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private final static DoubleWritable rating = new DoubleWritable();
        private Text developerId = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String developerIdStr = tokens[12]; // Index of DeveloperId
            String averageUserRatingStr = tokens[14]; // Index of Average_User_Rating

            // Check if both fields are not empty
            if (!developerIdStr.isEmpty() && !averageUserRatingStr.isEmpty()) {
                developerId.set(developerIdStr);
                try {
                    double averageUserRating = Double.parseDouble(averageUserRatingStr);
                    rating.set(averageUserRating);
                    context.write(developerId, rating);
                } catch (NumberFormatException e) {
                    // Handle invalid format gracefully, you can skip this record or log an error
                    // For simplicity, we're just ignoring it here
                }
            }
        }
    }

    public static class DoubleAverageReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();
        private int count = 0;

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            count = 0; // Reset count for each developer ID
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            double average = sum / count;
            result.set(average);
            context.write(key, new DoubleWritable(count)); // Emit count first
            context.write(key, result); // Then emit average
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "developer rating correlation");
        job.setJarByClass(AppRatingCorrelation.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(DoubleAverageReducer.class);
        job.setReducerClass(DoubleAverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}