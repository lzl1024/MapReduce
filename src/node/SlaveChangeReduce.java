package node;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;

import dfs.FileTransmitServer;

import mapreduce.MapperPerform;
import socket.ChangeReduceMsg;
import socket.CompleteMsg;
import socket.Message;
import socket.Message.MSG_TYPE;
import util.Constants;

public class SlaveChangeReduce extends Thread {

    ChangeReduceMsg msg = null;

    public SlaveChangeReduce(ChangeReduceMsg myMsg) {
        this.msg = myMsg;
    }

    public void run() {
        if (msg != null) {
            SocketAddress oldSocketAddr = msg.getOld();
            SocketAddress newSocketAddr = msg.getNew();
            System.out.println("old sockADDR" + oldSocketAddr);
            System.out.println("new sockADDR" + newSocketAddr);
            for (int i = 0; i < SlaveCompute.mapperThreadList.size(); i++) {
                MapperPerform newMapper = (MapperPerform) SlaveCompute.mapperThreadList
                        .get(i);
                ArrayList<SocketAddress> list = newMapper.getReduceList();
                System.out.println("reducelist is" + list);
                int index = list.indexOf(oldSocketAddr);
                if(index != -1)
                	list.set(index, newSocketAddr);
                
            }
            Socket sock = new Socket();
            try {
				
				ArrayList<String> failedFiles = SlaveCompute.failedCache.get(oldSocketAddr);
				if(failedFiles != null) {
					for(String e : failedFiles) {
						sock.connect(newSocketAddr);
						new Message(MSG_TYPE.FILE_DOWNLOAD, new CompleteMsg(e, SlaveListen.sockComMsg, null)).send(sock, null, -1);
						FileTransmitServer.sendFile(e, sock);
						sock.close();
					}
				}
			} catch (Exception e) {
				System.out.println("send files in failedCache failure.");
				
				try {
					Socket socket = new Socket(Constants.MasterIp, Constants.SlaveActivePort);
					new Message(MSG_TYPE.NODE_FAIL, newSocketAddr).send(socket, null, -1);
					socket.close();
				} catch (Exception e1) {
					System.out.println("Yourself failed");
					System.exit(-1);
				}
				
				e.printStackTrace();
			}
            
        }
    }
}
