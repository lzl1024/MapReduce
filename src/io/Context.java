package io;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

public class Context {

	private ArrayList<PrintWriter> writerList = new ArrayList<PrintWriter>();
	private int size;
	private boolean close;

	public Context(int size, String splitName) {
		for (int i = 1; i <= size; i++) {
			try {
				writerList.add(new PrintWriter(new FileWriter(splitName + "_"
						+ i), true));
			} catch (IOException e) {
				System.out.println("Internal Error");
				e.printStackTrace();
			}
		}
		close = false;
	}

	public void write(Writable<?> key, Writable<?> value) {
		// get proper writer
		PrintWriter out = writerList.get(key.hashCode() % size);
		out.println(key.get() + " // " + value.get());
	}
	
	public void close() {
		for(PrintWriter out : writerList) {
			out.close();
		}
		close = true;
	}
	
	public boolean isClose(){
		return close;
	}
}
