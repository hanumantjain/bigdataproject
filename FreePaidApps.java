package org.words;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FreePaidApps {

    public static class TokenizerMapper
            extends Mapper<Object, Text, BooleanWritable, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private BooleanWritable free = new BooleanWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length > 12) { // ensure it's a valid record
                String freeString = tokens[12].toLowerCase();
                if (freeString.equals("true")) {
                    free.set(true);
                } else if (freeString.equals("false")) {
                    free.set(false);
                }
                context.write(free, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<BooleanWritable,IntWritable,BooleanWritable,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(BooleanWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "free count");
        job.setJarByClass(FreePaidApps.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(BooleanWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

