package socket;

import java.io.Serializable;
import java.net.SocketAddress;
import java.util.ArrayList;

public class MapperAckMsg implements Serializable {

	/**
	 * Acknowledge mapper message
	 */
	private static final long serialVersionUID = 1L;

	private ArrayList<SocketAddress> reudcerList;
	private String mapperClass;
	private String splitName;

	public MapperAckMsg(String splitName, String mapperClass,
			ArrayList<SocketAddress> reducerList) {
		this.splitName = splitName;
		this.mapperClass = mapperClass;
		this.reudcerList = reducerList;
	}

	public ArrayList<SocketAddress> getReudcerList() {
		return reudcerList;
	}

	public void setReudcerList(ArrayList<SocketAddress> reudcerList) {
		this.reudcerList = reudcerList;
	}

	public String getMapperClass() {
		return mapperClass;
	}

	public void setMapperClass(String mapperClass) {
		this.mapperClass = mapperClass;
	}

	public String getSplitName() {
		return splitName;
	}

	public void setSplitName(String splitName) {
		this.splitName = splitName;
	}

}
