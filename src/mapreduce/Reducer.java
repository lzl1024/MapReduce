package mapreduce;

import io.Context;
import io.Text;
import io.Writable;

public abstract class Reducer {
    public abstract void reduce(Text key, Iterable<Writable<?>> values, Context context);
}
