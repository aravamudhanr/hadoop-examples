package org.raghav.tutorials.mutualfriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.raghav.tutorials.mutualfriends.writables.FriendArray;
import org.raghav.tutorials.mutualfriends.writables.FriendPair;

public class MutualFriendsDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        //Configuration processed by ToolRunner
        Configuration conf = getConf();

        //Define MapReduce job
        Job job = Job.getInstance(conf);
        job.setJarByClass(MutualFriendsDriver.class);
        job.setJobName("MutualFriends");

        //Set input and output locations
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //Set Input and Output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //Set Mapper and Reduce classes
        job.setMapperClass(MutualFriendsMapper.class);
        job.setReducerClass(MutualFriendsReducer.class);

        //Output types
        job.setOutputKeyClass(FriendPair.class);
        job.setOutputValueClass(FriendArray.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new MutualFriendsDriver(), args);
        System.exit(exitCode);
    }
}