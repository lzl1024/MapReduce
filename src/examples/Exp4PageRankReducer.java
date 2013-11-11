package examples;

import io.Context;
import io.LongWritable;
import io.Text;
import io.Writable;
import mapreduce.Reducer;

public class Exp4PageRankReducer extends Reducer{
    @Override
    public void reduce(Writable<?> key, Iterable<Writable<?>> values,
            Context context) {
        long pageRank = 0;
        for (Writable<?> value : values) {
            pageRank += ((LongWritable) value).get();
        }

        context.write((Text) key, new LongWritable(pageRank));
    }
}
