package node;

import java.net.SocketAddress;
import java.util.ArrayList;

import mapreduce.MapperPerform;
import socket.ChangeReduceMsg;

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
        }
    }
}
