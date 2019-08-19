package distiributed;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLConnection;

public class Receiver extends Thread {

    private static final int PORT = 1290 ;
    private Transporter.ReceiverListener cBack;
    private boolean stop = false;
    private Socket server;
    private ServerSocket serverSocket;

    public Receiver(Transporter.ReceiverListener cBack){
        this.cBack = cBack;
    }

    public void stopWorking() {
        stop = true;
        closeSockets();
        //TODO ..
    }

    private void closeSockets(){
        try {
            if(server != null && !server.isClosed())
                server.close();
            if(serverSocket != null && !serverSocket.isClosed())
                serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(){
        try {
            serverSocket = new ServerSocket(PORT);
            server = serverSocket.accept();
            while (!stop){
                ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(server.getInputStream()));
                int length = in.readInt();
                if(length>0) {
                    byte[] data = new byte[length];
                    in.readFully(data, 0, data.length);
                    SendItem sendItem = SendItem.fromByte(data);
                    cBack.gotResult(sendItem);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        closeSockets();
    }
}
