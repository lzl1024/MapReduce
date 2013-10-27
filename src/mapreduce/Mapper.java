package mapreduce;

import io.Context;
import io.Text;
import io.Writable;

public abstract class Mapper{
    public abstract void map(Writable key, Text value, Context context);
}
