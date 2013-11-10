package node;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;

import mapreduce.MapperPerform;
import socket.ChangeReduceMsg;
import socket.Message;
import socket.Message.MSG_TYPE;

public class SlaveChangeReduce extends Thread {

    ChangeReduceMsg msg = null;

    public SlaveChangeReduce(ChangeReduceMsg myMsg) {
        this.msg = myMsg;
    }

    public void run() {
        if (msg != null) {
            SocketAddress oldSocketAddr = msg.getOld();
            SocketAddress newSocketAddr = msg.getNew();
            for (int i = 0; i < SlaveCompute.mapperThreadList.size(); i++) {
                MapperPerform newMapper = (MapperPerform) SlaveCompute.mapperThreadList
                        .get(i);
                ArrayList<SocketAddress> list = newMapper.getReduceList();
                int index = list.indexOf(oldSocketAddr);
                list.set(index, newSocketAddr);
            }
            Socket sock = new Socket();
            try {
				sock.connect(newSocketAddr);
				ArrayList<String> failedFiles = SlaveCompute.failedCache.get(oldSocketAddr);
				for(String e : failedFiles) {
					new Message(MSG_TYPE.FILE_DOWNLOAD, e).send(sock, null, -1);
				}
			} catch (Exception e) {
				System.out.println("send files in failedCache failure.");
				e.printStackTrace();
			}
            
        }
    }
}
