package io;

public class LongWritable implements Writable<Long>{
    /**
     * Long wrapper in map Reduce
     */
    private static final long serialVersionUID = 1L;
    private Long field;

    public LongWritable() {
    }

    public LongWritable(Long field) {
        this.field = field;
    }

    @Override
    public Long get() {
        return field;
    }

    @Override
    public void set(Long field) {
        this.field = field;
    }

    @Override
    public void parse(String fields) {
        field = Long.parseLong(fields.trim());
    }

    @Override
    public int hashCode() {
        return field.hashCode();
    }
}
