package socket;

import java.io.Serializable;

public class ReducerAckMsg implements Serializable {

	/**
	 * Acknowledge reducer message
	 */
	private static final long serialVersionUID = 1L;
	
	private int mapperNum;
	private String ReducerClass;
	
	public int getMapperNum() {
		return mapperNum;
	}
	public void setMapperNum(int mapperNum) {
		this.mapperNum = mapperNum;
	}
	public String getReducerClass() {
		return ReducerClass;
	}
	public void setReducerClass(String reducerClass) {
		ReducerClass = reducerClass;
	}
}
