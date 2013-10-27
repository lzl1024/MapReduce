package io;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * 
 * Writable interface: send fields to output or receive fields from input
 *
 */
public abstract interface Writable extends Serializable {
    /**
     * Serialize the fields of this object to out
     * @param output
     */
    public void write(OutputStream output);

    /**
     * Deserialize the fields of this object from in
     * @param in
     */
    public void readFields(InputStream in);
    
    /**
     * get the field
     * @return
     */
    public Object get();
}
