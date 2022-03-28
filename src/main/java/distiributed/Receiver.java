package distiributed;

import optimizer.EngineRotater2;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Receiver extends Thread {

    private static final int BASE_PORT = 49158;
    private int port;
    private final String host;
    private Transporter transporter;
    private boolean stop = false;
    private Socket server;
    private ServerSocket serverSocket;
    private final int id;
    private boolean testMode  =false;
    private final EngineRotater2 optimizer;
    public Transporter.DataReceivedListener replicationListener;

    private RecieverReadyListener recieverReadyListener;
    public interface RecieverReadyListener{
        void recieverReady(int id);
    }


    public Receiver(Transporter transporter, String host, int id , EngineRotater2 optimizer , RecieverReadyListener recieverReadyListener) {
        this.transporter = transporter;
        this.host = host;
        String[] arr = host.split("\\.");
        this.port = Integer.valueOf(arr[arr.length - 1]) + BASE_PORT;
        this.id = id;
        this.optimizer = optimizer;
        this.recieverReadyListener = recieverReadyListener;
    }

    public void stopWorking() {
        stop = true;
        closeSockets();
        //TODO ..
    }


    private void closeSockets() {
        try {
            if (server != null && !server.isClosed())
                server.close();
            if (serverSocket != null && !serverSocket.isClosed())
                serverSocket.close();
            System.out.println("Reciver socket with " + host + ":" + port + " is closed ");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            try {
                server = serverSocket.accept();
                ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(server.getInputStream()));
                System.out.println("ready to receive from remote at port:" + port);
                informReady();
                while (!stop) {

                    int length = in.readInt();
                    System.err.println("got something .. length "+length);

                    if(length == Transporter.QUERIES_SESSION){
                        int count = in.readInt();
                        int batchID = in.readInt();
                        for(int i = 0 ; i < count ; i++){
                            int queryNo = in.readInt();
                            String query = in.readUTF();
                            transporter.recievedQuery(query , queryNo , count , batchID);
                        }
                        continue;
                    }

                    if(length == Transporter.QUERIES_DONE_BATCH){
                        int count = in.readInt();
                        for(int i = 0 ; i < count ; i++){
                            int queryNo = in.readInt();
                            transporter.recievedQueryDone(queryNo);
                        }
                        continue;
                    }
                    if(length == Transporter.QUERY_MSG){
                        System.out.println("Received Query from "+host);
                        int queryNo = in.readInt();
                        String query = in.readUTF();
                        System.out.println("Received Query from "+host + " "+query);
                        transporter.recievedQuery(query , queryNo , -1 ,0);
                    }
                    if (length == Transporter.PING_MESSAGE) {
                        System.out.println("recieved ping msg from " + host + ":" + port);
                        transporter.pingBack(id);
                        continue;
                    }
                    if (length == Transporter.PING_REPLY_MESSAGE) {
                        System.out.println("recieved ping reply msg from " + host + ":" + port);
                        transporter.gotPingReply(id);
                        continue;
                    }
                    if (length == Transporter.TEST_MESSAGE) {
                        length = in.readInt();
                        System.out.println("recieved test msg from " + host + ":" + port);
                        testMode = true;
                        //continue;
                    }
                    if (length == Transporter.TEST_REPLY_MESSAGE) {
                        transporter.gotTestReplyMsg(id);
                        System.out.println("recieved test reply msg from " + host + ":" + port);
                        continue;
                    }
                    if(length == Transporter.REPLICATION_REQUEST){
                        int dist = in.readInt();
                        int from = in.readInt();
                        int to = in.readInt();
                        optimizer.sendRequiredReplications(id , dist , from , to);
                        continue;
                    }
                    if(length == Transporter.SEND_REPLICATION_BACK || length == Transporter.FINISHED_SENDING_REPLICATION_ON_DIST){
                        int lengthT = in.readInt();
                        byte[] data = new byte[lengthT];
                        in.readFully(data, 0, data.length);
                        SendItem sendItem = SendItem.fromByte(data);
                        sendItem.queryNo = length;
                        replicationListener.gotData(sendItem);
                        continue;
                    }

                    if(length == Transporter.SEND_FULL_INDEX){
                        byte indexType = in.readByte();
                        int count = in.readInt();
                        int [] arr = new int[count];
                        for(int j = 0 ; j < count ; j++){
                            arr[j] = in.readInt();
                        }
                        optimizer.recievedIndexBatch(arr , indexType);
                        continue;
                    }

                    if(length == Transporter.SEND_FULL_INDEX_FINSHED){
                        optimizer.recievedIndexBatch(null ,(byte)0);
                        continue;
                    }

                    if (length > 0) {
                        byte[] data = new byte[length];
                        in.readFully(data, 0, data.length);
                        SendItem sendItem = SendItem.fromByte(data);
                        if (transporter != null){
                            if(!testMode)
                                transporter.receiverGotResult(sendItem);
                            else
                                transporter.replyTestMssg(data,id);
                        }

                    testMode = false;
                        /*if (host.matches("172.20.32.8")) { //TODO remove remove remove...
                            System.out.println("sending it back");
                            transporter.sendToAll(sendItem);
                        }*/
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        closeSockets();
    }

    private void informReady() {
        recieverReadyListener.recieverReady(this.id);
    }
}
