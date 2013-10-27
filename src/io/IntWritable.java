package io;

import java.io.InputStream;
import java.io.OutputStream;


public class IntWritable implements Writable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private int field;

    public IntWritable(int field){
        this.field = field;
    }

    @Override
    public void write(OutputStream output) {
        // TODO Auto-generated method stub
        
    }


    @Override
    public void readFields(InputStream in) {
        // TODO Auto-generated method stub
        
    }


    @Override
    public Object get() {
        return field;
    }

}
