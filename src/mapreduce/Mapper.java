package mapreduce;

import io.Context;
import io.IntWritable;
import io.Text;

public abstract class Mapper {
    public abstract void map(IntWritable key, Text value, Context context);
}
