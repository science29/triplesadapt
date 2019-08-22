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

public class Sender{

    private final static int BASE_PORT = 1290;
    private final Transporter transporter;
    private int port ;
    private static final int THREAD_COUNT_PER_HOST = 2;
    private final String host;
    private final int hostID;
    private boolean connectionVerfied = false;

    public String connectionMsg;

    private final ArrayList<SenderThread> threads ;

    private BlockingQueue<SendItem> sharedWorkQueue = new ArrayBlockingQueue(10000);

    public Sender(String host , String myIp , Transporter transporter , int toHostID) {
        this.host = host;
        this.hostID = toHostID;
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

    public void pingBack() {
        try {
            sharedWorkQueue.put(new SendItem(Transporter.PING_REPLY_MESSAGE , null ,null));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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

      /*  private void send(SendItem sendItem) {
            try {

                ArrayList<Integer> list  = new ArrayList<>();
                sendItem.serialize(list);
                outToServer.writeInt(list.size());
                for(int i =0 ; i < list.size() ; i++){
                    outToServer.writeInt(list.get(i));
                }
                outToServer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/

        private void send(SendItem sendItem) {
            try {
                if(sendItem.queryNo == Transporter.PING_MESSAGE){
                    outToServer.writeInt(Transporter.PING_MESSAGE);
                }
                if(sendItem.queryNo == Transporter.PING_REPLY_MESSAGE){
                    outToServer.writeInt(Transporter.PING_REPLY_MESSAGE);
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
