package socket;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;

/**
 * This class is the common type of message
 * 
 * @param MSG_TYPE
 *            type
 * @param Serializable
 *            content
 * 
 */
public class Message implements Serializable {

    private static final long serialVersionUID = 1L;

    public static enum MSG_TYPE {
        FILE_SPLIT_REQ, SLAVE_QUIT, KEEP_ALIVE, NODE_FAIL, RANDOM_RECORD,
        MAPPER_COMPLETE, WORK_COMPLETE, WORK_FAIL, NEW_JOB, WORK_KILLED,
        MAPPER_REQ, REDUCER_REQ, REDUCER_COMPLETE, FILE_DOWNLOAD, NOTIFY_PORT, 
        CHANGE_REDUCELIST, FILE_REQ, GET_FILE, PUT_FILE, PUT_FILE_FS, DELETE_FILE,      
    }

    // fields
    private MSG_TYPE type;
    Serializable content;

    public Message(MSG_TYPE type, Serializable content) {
        this.type = type;
        this.content = content;
    }

    public MSG_TYPE getType() {
        return type;
    }

    public Object getContent() {
        return content;
    }

    /**
     * send itself to remote host
     * 
     * @param reusedSocket
     * @param Ipaddr
     * @param port
     * @return
     * @throws Exception
     */
    public Socket send(Socket reusedSocket, String Ipaddr, int port)
            throws Exception {
        Socket sock = reusedSocket;
        if (sock == null) {
            sock = new Socket(Ipaddr, port);
        }
        ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
        out.writeObject(this);
        out.flush();

        return sock;
    }

    /**
     * receive message from remote host
     * 
     * @param reusedSocket
     * @param Ipaddr
     * @param port
     * @return
     * @throws Exception
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("resource")
    public static Message receive(Socket reusedSocket, String Ipaddr, int port)
            throws Exception {
        Message msg = null;
        Socket sock = reusedSocket;
        if (sock == null) {
            sock = new Socket(Ipaddr, port);
        }

        ObjectInputStream in = new ObjectInputStream(sock.getInputStream());
        msg = (Message) in.readObject();

        return msg;
    }
}
