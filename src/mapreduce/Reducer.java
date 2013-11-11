package mapreduce;

import io.Context;
import io.Writable;

public abstract class Reducer {
    public abstract void reduce(Writable<?> key, Iterable<Writable<?>> values,
            Context context);
}
