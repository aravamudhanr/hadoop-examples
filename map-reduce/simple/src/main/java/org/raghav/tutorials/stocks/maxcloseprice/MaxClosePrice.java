package org.raghav.tutorials.stocks.maxcloseprice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxClosePrice extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        //Configuration processed by ToolRunner
        Configuration conf = getConf();

        //Define MapReduce job
        Job job = Job.getInstance(conf);
        job.setJarByClass(MaxClosePrice.class);
        job.setJobName("MaxClosePrice");

        //Set input and output locations
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //Set Input and Output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //Set Mapper and Reduce classes
        job.setMapperClass(MaxClosePriceMapper.class);
        job.setReducerClass(MaxClosePriceReducer.class);

        //Output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new MaxClosePrice(), args);
        System.exit(exitCode);
    }
}