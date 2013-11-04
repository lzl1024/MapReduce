package socket;

import java.io.Serializable;
import java.net.SocketAddress;
import java.util.ArrayList;

/**
 * This kind of msg is used when we want to change the reduceList of a mapper
 */
public class ChangeReduceMsg implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private SocketAddress oldReduceTask;
	private SocketAddress newReduceTask;
	
	public ChangeReduceMsg(SocketAddress oldTask, SocketAddress newTask) {
		this.oldReduceTask = oldTask;
		this.newReduceTask = newTask;
	}
	public SocketAddress getOld() {
		return this.oldReduceTask;
	}
	public SocketAddress getNew() {
		return this.newReduceTask;
	}
}
