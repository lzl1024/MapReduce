package io;

public class IntWritable implements Writable<Integer> {

    /**
     * Integer wrapper in map Reduce
     */
    private static final long serialVersionUID = 1L;
    private Integer field;

    public IntWritable() {
    }

    public IntWritable(Integer field) {
        this.field = field;
    }

    @Override
    public Integer get() {
        return field;
    }

    @Override
    public void set(Integer field) {
        this.field = field;
    }

    @Override
    public void parse(String fields) {
        field = Integer.parseInt(fields.trim());
    }

    @Override
    public int hashCode() {
        return field.hashCode();
    }
}
