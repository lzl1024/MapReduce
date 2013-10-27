package examples;

import io.Context;
import io.IntWritable;
import io.Text;
import io.Writable;
import mapreduce.Mapper;

/**
 * 
 * Simple word count mapper example
 *
 */
public class Exp1WordCountMapper extends Mapper{

    @Override
    public void map(Writable key, Text value, Context context) {
        String line = value.toString();
        String[] words = line.split("\\s+");

        IntWritable fixVal = new IntWritable(1);
        for (String word : words) {
            context.write(new Text(word), fixVal);
        }
    }

}
