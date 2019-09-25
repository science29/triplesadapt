package distiributed;

import javax.swing.*;
import java.awt.*;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

public class Sender{

    private final static int BASE_PORT = 49158;
    private final Transporter transporter;
    private int port ;
    private static final int THREAD_COUNT_PER_HOST = 1;
    private final String host;
    private final int hostID;
    private final String myHost ;
    private boolean connectionVerfied = false;

    public String connectionMsg;

    private final ArrayList<SenderThread> threads ;

    private BlockingQueue<SendItem> sharedWorkQueue = new ArrayBlockingQueue(10000);

    public Sender(String host , String myIp , Transporter transporter , int toHostID) {
        this.host = host;
        this.hostID = toHostID;
        this.myHost = myIp;
        String [] arr = myIp.split("\\.");
        port = Integer.valueOf(arr[arr.length-1])+BASE_PORT;
        this.threads = new ArrayList<>();
        this.transporter = transporter;
        for(int i = 0 ; i < THREAD_COUNT_PER_HOST ; i++){
            SenderThread senderThread = new SenderThread();
            threads.add(senderThread);
            senderThread.start();
        }

    }

    public void addWork(SendItem sendItem) {
        try {
            sharedWorkQueue.put(sendItem);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void ping() {
        try {
            sharedWorkQueue.put(new SendItem(Transporter.PING_MESSAGE , null ,null));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void sendQuery(String query ,int queryNo , boolean lastOne ){
        try {
            if(lastOne)
                sharedWorkQueue.put(new SendItem(queryNo , query));
            else
                sharedWorkQueue.put(new SendItem(-1 * queryNo , query));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void sendQueryList(ArrayList<Integer> queriesNumberList , ArrayList<String> queryList){
        try {
            sharedWorkQueue.put(new SendItem(null , queriesNumberList));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void sendQueryListDone(ArrayList<Integer> queriesNumberList){
        try {
            sharedWorkQueue.put(new SendItem(null , queriesNumberList));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }




    public void pingBack() {
        try {
            sharedWorkQueue.put(new SendItem(Transporter.PING_REPLY_MESSAGE , null ,null));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void sendTestMesgBack() {
        try {
            sharedWorkQueue.put(new SendItem(Transporter.TEST_REPLY_MESSAGE , null ,null));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public long sendTestMesg(int length) {
        try {
            SendItem sendItem = new SendItem(Transporter.TEST_MESSAGE , null ,null);
            sendItem.generateTestData(length);
            sharedWorkQueue.put(sendItem);
            return System.nanoTime();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return 0;
    }




    private class SenderThread extends Thread {
        private boolean stop = false;
        private Socket socket ;
        private boolean working = false;
        ObjectOutputStream outToServer ;
        boolean socketOpened = false;

        @Override
        public void run() {
            working = true;
            openSocket();
            testConnection();
            /*if(myHost.matches("172.20.32.8")) {
                testConnectionAdvanced(100 , new ArrayList<>());
            }
            if(myHost.matches("172.20.32.8")) {
                testConnectionAdvanced2(100*Math.pow(1.3 ,16) , 100);
            }*/
            while (!stop) {
                try {
                    working = false;
                    SendItem sendItem = sharedWorkQueue.take();
                    if (stop)
                        break;
                    if(!socketOpened)
                        openSocket();
                    working = true;
                    send(sendItem);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    working = false;
                }
            }
            closeSocket();
        }

        private void testConnection() {
            long startTime = System.nanoTime();
            transporter.listenForPingReply(hostID, new PingListener() {
                @Override
                public void onPingReply() {
                    long stopTime = System.nanoTime();;
                    long elapsedTimeS = (stopTime - startTime) / 1000;
                    connectionVerfied = true;
                    connectionMsg = "ping to "+host+ " took: "+elapsedTimeS+" Ms";
                    System.out.println(connectionMsg);
                   /* if (GraphicsEnvironment.isHeadless()) {
                        // non gui mode
                    } else {
                        // gui mode
                        JOptionPane.showMessageDialog(null, "ping to "+host+ " took: "+elapsedTimeS+" Ms");
                    }*/

                }
            });
            ping();
        }



        HashMap<Double , Long> startTimeMap = new HashMap<>();

        private void testConnectionAdvanced(double length, ArrayList<String> res) {
            if(length > 20000000 || res.size() > 40) {
                for(int i = 0 ; i < res.size() ; i++)
                    System.out.println(res.get(i));
                return;
            }

            transporter.listenForPingReply(hostID, new PingListener() {
                @Override
                public void onPingReply() {
                    long stopTime = System.nanoTime();;
                    long elapsedTimeS = (stopTime - startTimeMap.get(length)) / 1000;
                    connectionMsg = length+","+elapsedTimeS;
                    res.add(connectionMsg);
                    testConnectionAdvanced(length * 1.3 , res);
                   /* if (GraphicsEnvironment.isHeadless()) {
                        // non gui mode
                    } else {
                        // gui mode
                        JOptionPane.showMessageDialog(null, "ping to "+host+ " took: "+elapsedTimeS+" Ms");
                    }*/

                }
            });
            startTimeMap.put(length ,   sendTestMesg((int)length) );
        }


        private long startTime;
        private void testConnectionAdvanced2(double length, int count) {

            transporter.listenForPingReply(hostID, new PingListener() {
                @Override
                public void onPingReply() {
                    long stopTime = System.nanoTime();;
                    long elapsedTimeS = (stopTime - startTime) / 1000;
                    connectionMsg = length+","+elapsedTimeS;
                    System.out.println(connectionMsg);
                   /* if (GraphicsEnvironment.isHeadless()) {
                        // non gui mode
                    } else {
                        // gui mode
                        JOptionPane.showMessageDialog(null, "ping to "+host+ " took: "+elapsedTimeS+" Ms");
                    }*/

                }
            });
            startTime =  sendTestMesg((int)length);
            for(int i = 0 ; i < count ; i++)
                sendTestMesg((int)length);
        }



        private void closeSocket() {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void stopWorking() {
            stop = true;
            addWork(new SendItem()); //TODO not a correct way!
        }

        private void openSocket() {
            while (!socketOpened) {
                try {
                    socket = new Socket(host, port);
                    outToServer = new ObjectOutputStream(socket.getOutputStream());
                    socketOpened = true;
                } catch (IOException e) {
                    System.err.println("socket to " + host + " is not yet opened, trying in 10 sec");
                } catch (Exception e) {
                    System.err.println("socket to " + host + " is not yet opened, trying in 10 sec");
                }
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("socket to " + host + " established..");
        }




        private synchronized void send(SendItem sendItem) {
            try {

                if(sendItem.queriesNumberList != null){
                    if(sendItem.queries == null){
                        outToServer.writeInt(Transporter.QUERIES_DONE_BATCH);
                        outToServer.writeInt(sendItem.queriesNumberList.size());
                        for(int i = 0 ; i < sendItem.queriesNumberList.size() ; i++){
                            outToServer.writeInt(sendItem.queriesNumberList.get(i));
                        }
                        outToServer.flush();
                    }else{
                        outToServer.writeInt(Transporter.QUERIES_SESSION);
                        outToServer.writeInt(sendItem.queriesNumberList.size());
                        for(int i = 0 ; i < sendItem.queriesNumberList.size() ; i++){
                            outToServer.writeInt(sendItem.queriesNumberList.get(i));
                            outToServer.writeInt(sendItem.queries.get(i));
                        }
                        outToServer.flush();
                    }
                    return;
                }

                if(sendItem.msg != null){
                    boolean many = false;
                    outToServer.writeInt(Transporter.QUERY_MSG);
                    if(sendItem.queryNo < 0){
                        sendItem.queryNo = -1 * sendItem.queryNo;
                        many = true;
                    }
                    outToServer.writeInt(sendItem.queryNo);
                    outToServer.writeUTF(sendItem.msg);
                    if(!many)
                        outToServer.flush();
                    return;
                }
                if(sendItem.queryNo == Transporter.PING_MESSAGE){
                    System.out.println("pinging to "+host+":"+port);
                    outToServer.writeInt(Transporter.PING_MESSAGE);
                    outToServer.flush();
                    return;
                }
                if(sendItem.queryNo == Transporter.PING_REPLY_MESSAGE){
                    System.out.println("pinging back to "+host+":"+port);
                    outToServer.writeInt(Transporter.PING_REPLY_MESSAGE);
                    outToServer.flush();
                    return;
                }
                if(sendItem.queryNo == Transporter.TEST_REPLY_MESSAGE){
                    System.out.println("testing back to "+host+":"+port);
                    outToServer.writeInt(Transporter.TEST_REPLY_MESSAGE);
                    outToServer.flush();
                    return;
                }
                if(sendItem.queryNo == Transporter.TEST_MESSAGE){
                    System.out.println("sending test message to "+host+":"+port);
                    outToServer.writeInt(Transporter.TEST_MESSAGE);
                }
                byte [] data = sendItem.getBytes();
                outToServer.writeInt(data.length);
                outToServer.write(data);
                outToServer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public interface PingListener{
        void onPingReply();
    }
}
