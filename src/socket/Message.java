package socket;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

/**
 * This class is the common type of message
 * @param MSG_TYPE type
 * @param Serializable content
 *
 */
public class Message implements Serializable {

    private static final long serialVersionUID = 1L;

    public static enum MSG_TYPE {
        FILE_SPLIT_REQ, FILE_SPLIT_ACK
    }
    
    //fields
    private MSG_TYPE type;
    Serializable content;

    public Message (MSG_TYPE type, Serializable content) {
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
     * @param reusedSocket
     * @param Ipaddr
     * @param port
     * @return
     * @throws IOException 
     * @throws UnknownHostException 
     */
    public Socket send(Socket reusedSocket, String Ipaddr, int port) {
        Socket sock = reusedSocket;
        try {
            if (sock == null) {
                sock = new Socket(Ipaddr, port);
            }
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
            out.writeObject(this);
            out.flush();

        } catch(Exception e) {
        	e.printStackTrace();
        }

        return sock;
    }

    /**
     * receive message from remote host
     * @param reusedSocket
     * @param Ipaddr
     * @param port
     * @return
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    @SuppressWarnings("resource")
	public static Message receive(Socket reusedSocket, String Ipaddr, int port) 
			throws SocketTimeoutException {
        Message msg = null;
        Socket sock = reusedSocket;
        try{
            if (sock == null) {
                sock = new Socket(Ipaddr, port);
            }
    
            ObjectInputStream in = new ObjectInputStream(sock.getInputStream());
            msg = (Message) in.readObject();
            
        } catch (Exception e) {
        	e.printStackTrace();
        }
        
        return msg;
    }
}
