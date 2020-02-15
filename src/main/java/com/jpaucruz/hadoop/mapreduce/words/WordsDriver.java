package com.jpaucruz.hadoop.mapreduce.words;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;

public class WordsDriver extends Configured implements Tool{
  
  private static final Log LOG = LogFactory.getLog(WordsDriver.class);

	public static void main(String[] args) throws Exception {
    int status = ToolRunner.run(new Configuration(), new WordsDriver(), args);
    System.exit(status);
	}
  
  public int run(String[] args) throws Exception {
  
	  // job configuration
    Job job = Job.getInstance(getConf());
    job.setJarByClass(WordsDriver.class);
    job.setMapperClass(WordsMap.class);
    job.setReducerClass(WordsReduce.class);
  
    // process arguments
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    String reducers = job.getConfiguration().get("num-reducers");

    // map configuration
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    // reduce configuration
    MultipleOutputs.addNamedOutput(job, "output", TextOutputFormat.class, Text.class, Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    if (reducers != null && reducers.length() > 0) {
      job.setNumReduceTasks(Integer.valueOf(reducers));
    } else {
      job.setNumReduceTasks(5);
    }

    LOG.info("Starting job: " + new Date());

    // launcher
    job.waitForCompletion(true);

    LOG.info("Finishing job: " + new Date());
  
    return 0;
	  
  }
  
}
