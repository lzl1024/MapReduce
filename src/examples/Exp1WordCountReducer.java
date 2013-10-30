package examples;

import io.Context;
import io.IntWritable;
import io.Text;
import io.Writable;
import mapreduce.Reducer;

/**
 * 
 * Simple word count reducer example
 *
 */
public class Exp1WordCountReducer extends Reducer {

    @Override
    public void reduce(Writable<?> key, Iterable<Writable<?>> values, Context context) {
        int wordCount = 0;
        for (Writable<?> value : values) {
            wordCount += ((IntWritable)value).get();
        }
        
        context.write((Text)key, new IntWritable(wordCount));
    }

}
