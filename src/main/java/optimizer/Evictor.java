package optimizer;

public class Evictor extends Thread{


    private final Optimiser optimizer;

    public Evictor(Optimiser optimiser){
        this.optimizer = optimiser;
    }

    @Override
    public void run(){

        //look for the highest important data  and replace it with lower importnat
        //optimizer.get


    }

}
