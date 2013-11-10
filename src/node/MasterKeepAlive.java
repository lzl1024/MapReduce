package node;

import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;

import socket.Message;
import util.Constants;

/**
 * 
 * Master will poll slaves with Keep-alive for every 5s in case of some mappers
 * fail without being reported
 */
public class MasterKeepAlive extends Thread {
    public void run() {
        while (true) {
            Message msg = new Message(Message.MSG_TYPE.KEEP_ALIVE, null);
            ArrayList<SocketAddress> failList = new ArrayList<SocketAddress>();
            // poll all slaves
            for (SlaveInfo info : MasterMain.slavePool.values()) {
                Socket sock = info.getSocket();
                try {
                    msg.send(sock, null, -1);
                    if (Message.receive(sock, null, -1).getType() != Message.MSG_TYPE.KEEP_ALIVE) {
                        throw new Exception();
                    }
                } catch (Exception e) {
                    // add to fail list
                    failList.add(sock.getRemoteSocketAddress());
                }
            }
            // handle failure
            if (failList.size() > 0) {
                MasterMain.handleLeave(failList);
            }

            try {
                sleep(Constants.KEEP_ALIVE_INT);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
