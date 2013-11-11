package examples;

import io.Context;
import io.IntWritable;
import io.LongWritable;
import io.Text;
import mapreduce.Mapper;

/**
 * 
 * Simple word count mapper example
 * 
 */
public class Exp1WordCountMapper extends Mapper {

    @Override
    public void map(LongWritable key, Text value, Context context) {
        String line = value.get();
        String[] words = line.split("\\s+");

        IntWritable fixVal = new IntWritable(1);
        for (String word : words) {
            context.write(new Text(word), fixVal);
        }
    }

}
