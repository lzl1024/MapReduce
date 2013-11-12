package node;

import java.io.File;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;

import mapreduce.MapperPerform;
import socket.ChangeReduceMsg;
import socket.CompleteMsg;
import socket.Message;
import socket.Message.MSG_TYPE;
import util.Constants;
import dfs.FileTransmitServer;

public class SlaveChangeReduce extends Thread {

    ChangeReduceMsg msg = null;

    public SlaveChangeReduce(ChangeReduceMsg myMsg) {
        this.msg = myMsg;
    }

    /**
     * the thread to change the reduce list from one address to another
     */
    public void run() {
        if (msg != null) {
            SocketAddress oldSocketAddr = msg.getOld();
            SocketAddress newSocketAddr = msg.getNew();
            
            // request the files that the old reducer has already have
            if(MapperPerform.sentFileMap.containsKey(oldSocketAddr)) {
            	ArrayList<String> tmpList = MapperPerform.sentFileMap.get(oldSocketAddr);
            	ArrayList<String> newList = new ArrayList<String>();
            	
            	for(String str : tmpList) {
            		File detectFile = new File(str);
            		if(detectFile.exists()) {
            			try {
            				Socket sock = new Socket();
            				sock.connect(newSocketAddr);
            				new Message(MSG_TYPE.FILE_DOWNLOAD, new CompleteMsg(str, SlaveListen.sockComMsg, null)).send(sock, null, -1);
            				FileTransmitServer.sendFile(str, sock);
            				if(!sock.isClosed())
            					sock.close();
            				newList.add(str);
            			} catch (Exception e) {
            				System.out.println("New reducer also failed");
            			}    
            		}
            	}
            	MapperPerform.sentFileMap.put(newSocketAddr, newList);
                MapperPerform.sentFileMap.remove(oldSocketAddr);
            }
            
            /**
             * check the reducelist to find which reducer should be replaced
             */
            for (int i = 0; i < SlaveCompute.mapperThreadList.size(); i++) {
                MapperPerform newMapper = (MapperPerform) SlaveCompute.mapperThreadList
                        .get(i);
                ArrayList<SocketAddress> list = newMapper.getReduceList();
                int index = list.indexOf(oldSocketAddr);
                if(index != -1)
                	list.set(index, newSocketAddr);
                
            }
            Socket sock = new Socket();
            ArrayList<String> failedFiles = SlaveCompute.failedCache.get(oldSocketAddr);

            try {			
				if(failedFiles != null) {
	                // remove the oldscoketAddr in failed Cache
	                SlaveCompute.failedCache.remove(oldSocketAddr);
                    for (int i = failedFiles.size()-1; i >=0 ; i--) {
                        String e = failedFiles.get(i);
						sock.connect(newSocketAddr);
						new Message(MSG_TYPE.FILE_DOWNLOAD, new CompleteMsg(e, SlaveListen.sockComMsg, null)).send(sock, null, -1);
						FileTransmitServer.sendFile(e, sock);
						sock.close();
						failedFiles.remove(i);
					}
				}		
			} catch (Exception e) {
				System.out.println("send files in failedCache failure.");
				// update the failedCache place
				SlaveCompute.failedCache.put(newSocketAddr, failedFiles);
				ArrayList<SocketAddress> tmp = new ArrayList<SocketAddress>();
				tmp.add(newSocketAddr);
				try {
					Socket socket = new Socket(Constants.MasterIp, Constants.SlaveActivePort);
					new Message(MSG_TYPE.NODE_FAIL, tmp).send(socket, null, -1);
					socket.close();
				} catch (Exception e1) {
					System.out.println("Yourself failed");
					System.exit(-1);
				}
				
			}
            
        }
    }
}
