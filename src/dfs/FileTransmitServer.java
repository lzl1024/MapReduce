package dfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URL;
import java.net.URLConnection;

import util.Constants;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

/**
 * This is the server handler to distribute file splits
 * 
 * @author zhuolinl dil1
 * 
 */
public class FileTransmitServer implements HttpHandler {

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().set("Content-Type",
                "text/html;charset=UTF-8");

        // get the url and ready for response
        String url = exchange.getRequestURI().getPath().substring(1);
        exchange.sendResponseHeaders(200, 0);
        FileInputStream fs = new FileInputStream(url);
        OutputStream out = exchange.getResponseBody();

        // write files into inputstream
        byte[] buffer = new byte[1024];
        int byteNum = 0;
        while ((byteNum = fs.read(buffer)) != -1) {
            out.write(buffer, 0, byteNum);
        }

        fs.close();
        out.flush();
        out.close();
    }

    /**
     * Download file from remote host
     * 
     * @param urlAddr
     * @param filename
     */
    public static void httpDownload(String urlAddr, String filename) {

        try {
            System.out.printf("Download %s to %s\n", urlAddr, filename);
            URLConnection conn = new URL(urlAddr).openConnection();
            InputStream in = conn.getInputStream();
            FileOutputStream fs = new FileOutputStream(filename);
            int byteNum = 0;

            byte[] buffer = new byte[1024];
            while ((byteNum = in.read(buffer)) != -1) {
                fs.write(buffer, 0, byteNum);
            }
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to download file from Server");
        }
    }

    /**
     * send files between slaves
     * 
     * @param fileName
     * @param socket
     * @throws IOException
     */
    public static void sendFile(String fileName, Socket socket)
            throws IOException {
        DataInputStream file = new DataInputStream(new BufferedInputStream(
                new FileInputStream(fileName)));
        DataOutputStream sockdata = new DataOutputStream(
                socket.getOutputStream());
        byte[] buf = new byte[Constants.BufferSize];
        int read_num;
        while ((read_num = file.read(buf)) != -1) {
            sockdata.write(buf, 0, read_num);
        }
        sockdata.flush();
        file.close();
        socket.close();
    }

    /**
     * Receive a file split from the remote
     * 
     * @param content
     * @throws IOException
     */
    public static void receiveFile(String fileName, Socket sock)
            throws IOException {
        DataInputStream sockData = new DataInputStream(new BufferedInputStream(
                sock.getInputStream()));
        DataOutputStream file = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(fileName)));
        byte[] buf = new byte[Constants.BufferSize];

        int readNum;
        while ((readNum = sockData.read(buf)) != -1) {
            file.write(buf, 0, readNum);
        }
        file.close();
    }
}
