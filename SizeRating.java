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

public class SizeRating {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable size = new IntWritable();
        private Text range = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 17) {
                int sizeMB = Integer.parseInt(fields[16]);
                if (sizeMB >= 0 && sizeMB <= 300) {
                    range.set((sizeMB / 10) * 10 + "-" + ((sizeMB / 10) * 10 + 10 > 300 ? "300+" : (sizeMB / 10) * 10 + 10));
                } else {
                    range.set("300+");
                }

                size.set(sizeMB);
                context.write(range, size);
            }
        }
    }

    public static class IntCountReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable count = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int totalCount = 0;
            for (IntWritable val : values) {
                totalCount++;
            }
            count.set(totalCount);
            context.write(key, count);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "app size range");
        job.setJarByClass(SizeRating.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntCountReducer.class);
        job.setReducerClass(IntCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
