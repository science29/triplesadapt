package distiributed;

import org.omg.PortableServer.POA;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLConnection;

public class Receiver extends Thread {

    private static final int BASE_PORT = 1290 ;
    private int port;
    private final String host;
    private Transporter transporter;
    private boolean stop = false;
    private Socket server;
    private ServerSocket serverSocket;
    private final  int id;

    public Receiver(Transporter transporter , String host , int id){
        this.transporter = transporter;
        this.host = host;
        String [] arr = host.split("\\.");
        this.port = Integer.valueOf(arr[arr.length - 1])+BASE_PORT;
        this.id = id;
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
            serverSocket = new ServerSocket(port);
            server = serverSocket.accept();
            System.out.println("ready to receive from remote ..");
            while (!stop){
                ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(server.getInputStream()));
                int length = in.readInt();
                if(length == Transporter.PING_MESSAGE){
                    transporter.pingBack(id);
                    continue;
                }
                if(length == Transporter.PING_REPLY_MESSAGE){
                    transporter.gotPingReply(id);
                    continue;
                }
                if(length>0) {
                    byte[] data = new byte[length];
                    in.readFully(data, 0, data.length);
                    SendItem sendItem = SendItem.fromByte(data);
                    if(transporter != null)
                        transporter.receiverGotResult(sendItem);
                    System.err.println("got something .. ");
                    //transporter.sendToAll(sendItem);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        closeSockets();
    }
}
