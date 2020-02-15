package com.jpaucruz.hadoop.mapreduce.words;

import com.jpaucruz.hadoop.mapreduce.words.utils.WordsCounters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordsMap extends Mapper <LongWritable, Text, Text, LongWritable> {
  
  private static final Log LOG = LogFactory.getLog(WordsMap.class);
  
  @Override
  public void map(LongWritable key, Text value, Context context){
    
    context.getCounter(WordsCounters.ANALYSIS.NUM_MAPPERS).increment(1);
    
    try{
  
      // input map
      String line = value.toString();
  
      // process
      String[] words = line.split(" ");
      for(final String word : words) {
        if (!word.isEmpty()){
          context.write(new Text(word), new LongWritable(1));
          context.getCounter(WordsCounters.ANALYSIS.NUM_LINES_MAP).increment(1);
        }
      }
      
    }catch (Exception ex){
      LOG.error(ex.getMessage());
    }
    
  }

}
