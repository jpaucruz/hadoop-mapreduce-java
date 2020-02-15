package com.jpaucruz.hadoop.mapreduce.words;

import com.jpaucruz.hadoop.mapreduce.words.utils.WordsCounters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class WordsReduce extends Reducer<Text, LongWritable, Text, Text> {
  
  private static final Log LOG = LogFactory.getLog(WordsReduce.class);
  private MultipleOutputs<Text, Text> outputs;
  
  @Override
  public void setup(Context context) throws IOException, InterruptedException{
    outputs = new MultipleOutputs<Text, Text>(context);
    context.getCounter(WordsCounters.ANALYSIS.NUM_REDUCER).increment(1);
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException{
    outputs.close();
  }
  
  
  @Override
  public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
    
    context.getCounter(WordsCounters.ANALYSIS.NUM_REDUCER_GROUPS).increment(1);
    
    // input
    LOG.info("Word: " + key.toString());
  
    // process
    Long total = 0l;
    for(final LongWritable value : values) {
      total = total + value.get();
    }

    // output
    outputs.write("output", null, new Text("Word: " + key.toString() + " - " + "Num: " + total));
    context.getCounter(WordsCounters.ANALYSIS.NUM_LINES_WRITE).increment(1);
  
  }
  
}
