package com.jpaucruz.hadoop.mapreduce.temperature;

import com.jpaucruz.hadoop.mapreduce.temperature.utils.TemperatureConsts;
import com.jpaucruz.hadoop.mapreduce.temperature.utils.TemperatureCounters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TemperatureMap extends Mapper <LongWritable, Text, Text, DoubleWritable> {
  
  private static final Log LOG = LogFactory.getLog(TemperatureMap.class);
  
  @Override
  public void map(LongWritable key, Text value, Context context){
    
    context.getCounter(TemperatureCounters.ANALYSIS.NUM_MAPPERS).increment(1);
    
    try{
  
      // input map
      String line = value.toString();
  
      // process
      String[] fields = line.split(",");
      if(!fields[TemperatureConsts.FIELDS.TEMPERATURE.ordinal()].isEmpty()) {
        context.write(
          new Text(fields[TemperatureConsts.FIELDS.CITY.ordinal()]),
          new DoubleWritable(new Double(fields[TemperatureConsts.FIELDS.TEMPERATURE.ordinal()]))
        );
        context.getCounter(TemperatureCounters.ANALYSIS.NUM_LINES_MAP).increment(1);
      }
      
    }catch (Exception ex){
      LOG.error(ex.getMessage());
    }
    
  }

}
