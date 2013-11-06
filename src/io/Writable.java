package io;

import java.io.Serializable;

/**
 * 
 * Writable interface: data wrapper in Map reduce
 * 
 */
public abstract interface Writable<T> extends Serializable {
    /**
     * get and set the field
     */
    public T get();

    public void set(T fields);

    // parse fields from string to what it needs
    void parse(String fields);
}
