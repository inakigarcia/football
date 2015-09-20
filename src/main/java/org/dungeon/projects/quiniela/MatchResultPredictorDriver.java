package org.dungeon.projects.quiniela;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import  org.apache.hadoop.util.Tool;
import  org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * hadoop jar football.jar org.dungeon.projects.quiniela.MatchResultCountDriver "/home/ranadion/work/football_in/" "/home/ranadion/work/football_out/"
 *
 */
public class MatchResultPredictorDriver extends Configured implements Tool {

    public int run(String[] args) throws Exception {
    
        if(args.length != 2) {
            System.out.println("Usage: " + getClass().getSimpleName() + "[generic options] <input dir> <output dir> \n");
            return -1;
        }
    
        Job job = Job.getInstance(getConf());        
        job.setJarByClass(MatchResultPredictorDriver.class);        
        job.setJobName("Match Result Counter");  
    
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
                 
        job.setMapperClass(MatchResultPredictorMapper.class);       
        job.setReducerClass(MatchResultPredictorReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
            
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new MatchResultPredictorDriver(), args);
        System.exit(exitCode);
    }

}