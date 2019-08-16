package distiributed;


import triple.TriplePattern2;

import java.util.ArrayList;

public class Transporter {





    public Transporter(){
        hosts = new ArrayList<>();
    }

    public void sendToAll(SendItem sendItem){
        sendersPool.addWork(sendItem);
    }


    public void receive(int queryNo , TriplePattern2 triplePattern2 , ReceiverListener cBack){

    }



    public interface ReceiverListener{
        void gotResult(SendItem sendItem);

    }
}
