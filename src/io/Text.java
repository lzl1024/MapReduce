package io;

public class Text implements Writable<String> {

    /**
     * String Wrapper in MapReduce
     */
    private static final long serialVersionUID = 1L;
    private String field;

    public Text() {
    }

    public Text(String field) {
        this.field = field;
    }

    @Override
    public String get() {
        return field;
    }

    @Override
    public void set(String fields) {
        this.field = fields;
    }

    @Override
    public void parse(String fields) {
        this.field = fields;
    }

    @Override
    public int hashCode() {
        return field.hashCode();
    }
    
    @Override
	public String toString() {
		return field.toString();
	}
}