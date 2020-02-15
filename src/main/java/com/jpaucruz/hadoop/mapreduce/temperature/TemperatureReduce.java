package com.jpaucruz.hadoop.mapreduce.temperature;

import com.jpaucruz.hadoop.mapreduce.temperature.utils.TemperatureCounters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class TemperatureReduce extends Reducer<Text, DoubleWritable, Text, Text> {
  
  private static final Log LOG = LogFactory.getLog(TemperatureReduce.class);
  private MultipleOutputs<Text, Text> outputs;
  
  @Override
  public void setup(Context context) throws IOException, InterruptedException{
    outputs = new MultipleOutputs<Text, Text>(context);
    context.getCounter(TemperatureCounters.ANALYSIS.NUM_REDUCER).increment(1);
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException{
    outputs.close();
  }
  
  
  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
    
    context.getCounter(TemperatureCounters.ANALYSIS.NUM_REDUCER_GROUPS).increment(1);
    
    // input
    LOG.info("City: " + key.toString());
  
    // process
    BigDecimal total = BigDecimal.ZERO;
    Long counter = 0l;
    for(final DoubleWritable temp : values) {
      total = total.add(BigDecimal.valueOf(temp.get()));
      counter ++;
    }
    BigDecimal average = total.divide(BigDecimal.valueOf(counter), 4, RoundingMode.HALF_UP);
  
    // output
    outputs.write("output", null, new Text("City: " + key.toString() + " - " + "Average: " + average));
    context.getCounter(TemperatureCounters.ANALYSIS.NUM_LINES_WRITE).increment(1);
  
  }
  
}
