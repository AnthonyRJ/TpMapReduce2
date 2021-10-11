package com.opstty.job;

import com.opstty.mapper.RoundingMapper;
import com.opstty.mapper.TreesSmallToLargeMapper;
import com.opstty.reducer.IntSumReducer;
import com.opstty.reducer.TreesSmallToLargeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TreesSmallToLarge {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: treesSmallToLarge <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "treesSmallToLarge");
        job.setJarByClass(TreesSmallToLarge.class);
        job.setMapperClass(TreesSmallToLargeMapper.class);
        job.setCombinerClass(TreesSmallToLargeReducer.class);
        job.setReducerClass(TreesSmallToLargeReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
