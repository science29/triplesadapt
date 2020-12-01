package optimizer.Rules;

import triple.Triple;

import java.util.ArrayList;

public abstract class OperationalRule {

    abstract TripleBlock getNextTriplesBlock();


    public class TripleBlock{
        public static final int block_size = 1000;
        private final byte destIndex;
        private ArrayList<Triple> triples;
        private double benefit;


        public TripleBlock(ArrayList<Triple> triples , double benefit, byte destiantionIndex){
            this.triples = triples;
            this.benefit = benefit;
            this.destIndex = destiantionIndex;
        }


        public double getBenefit(){
            return benefit;
        }
        public ArrayList<Triple> getTriplesList(){
            return triples;
        }
    }
}
