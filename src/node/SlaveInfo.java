package node;

import java.net.Socket;
import java.util.Comparator;
import java.util.concurrent.CopyOnWriteArrayList;

public class SlaveInfo {

	private Socket socket;
	// the layout of mapper tasks, value is split name
	private CopyOnWriteArrayList<String> mapperTasks;
	// the layout of reducer tasks, value is jobID
	private CopyOnWriteArrayList<String> reducerTasks;
	
	public SlaveInfo(){
		mapperTasks = new CopyOnWriteArrayList<String>();
		reducerTasks = new CopyOnWriteArrayList<String>();
	}
	
	public SlaveInfo(Socket sock) {
		mapperTasks = new CopyOnWriteArrayList<String>();
		reducerTasks = new CopyOnWriteArrayList<String>();
		socket = sock;
	}

	public Socket getSocket() {
		return socket;
	}

	public void setSocket(Socket socket) {
		this.socket = socket;
	}

	public CopyOnWriteArrayList<String> getMapperTasks() {
		return mapperTasks;
	}

	public void setMapperTasks(CopyOnWriteArrayList<String> mapperTasks) {
		this.mapperTasks = mapperTasks;
	}

	public CopyOnWriteArrayList<String> getReducerTasks() {
		return reducerTasks;
	}

	public void setReducerTasks(CopyOnWriteArrayList<String> reducerTasks) {
		this.reducerTasks = reducerTasks;
	}

	/**
	 * 
	 * two comparator to sort according to reducerTask and mapperTask
	 *
	 */
	public static class ReducerPrio implements Comparator<SlaveInfo>{

		@Override
		public int compare(SlaveInfo info1, SlaveInfo info2) {
			if (info1.reducerTasks.size() < info2.reducerTasks.size()) {
				return 1;
			} else if (info1.reducerTasks.size() > info2.reducerTasks.size()){
				return -1;
			}
			return 0;
		}
		
	}
	
	public static class MapperPrio implements Comparator<SlaveInfo>{

		@Override
		public int compare(SlaveInfo info1, SlaveInfo info2) {
			if (info1.mapperTasks.size() < info2.mapperTasks.size()) {
				return 1;
			} else if (info1.mapperTasks.size() > info2.mapperTasks.size()){
				return -1;
			}
			return 0;
		}
		
	}
}
