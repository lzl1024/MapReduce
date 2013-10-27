package io;

import java.io.InputStream;
import java.io.OutputStream;

public class Text implements Writable{
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private String field;

    public Text(String field){
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