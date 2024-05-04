package org.words;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgRatingAppUpdates {
    public static class ReviewMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            String appID = line[1];
            String avgRatingStr = line[14]; // Average_User_Rating
            String reviewsStr = line[15]; // Reviews

            try {
                double avgRating = Double.parseDouble(avgRatingStr);
                int reviews = Integer.parseInt(reviewsStr);
                context.write(new Text(appID), new IntWritable(reviews));
            } catch (NumberFormatException e) {
                // Handle invalid data (e.g., non-numeric ratings or reviews)
                // You can log or skip such records as needed
            }
        }
    }

    public static class ReviewReducer extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int totalReviews = 0;
            double totalRating = 0.0;

            for (IntWritable value : values) {
                totalReviews++;
                totalRating += value.get();
            }

            double avgRating = totalRating / totalReviews;
            String result = String.format("%.2f", avgRating);
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AvgRatingAppUpdates <input dir> <output dir>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Rating After Updates");
        job.setJarByClass(AvgRatingAppUpdates.class);

        job.setMapperClass(ReviewMapper.class);
        job.setReducerClass(ReviewReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

