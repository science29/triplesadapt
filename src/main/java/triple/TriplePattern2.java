package triple;

import java.util.HashMap;

public class TriplePattern2 {
    public final static int thisIsVariable = -1;




    private long triples[]= new long[3];
    public String stringTriple[] = new String[3] ;
    public long fixedTriples[] = new long[3];
    public HashMap<Long , Integer> variablesIndex;
    public String tempID ; //for debug purpose only

    private Triple triple;

    //int varaibles[] = new int[3];

    public TriplePattern2(long s , long p , long o){

        triple= new Triple(s,p,o);
    }

    public TriplePattern2(TriplePattern triplePattern){
        triples[0] = triplePattern.triples[0] ;
        triples[1] = triplePattern.triples[0];
        triples[2] = triplePattern.triples[0];
        fixedTriples[0] = fixedTriples[0] ;
        fixedTriples[1] = fixedTriples[1];
        fixedTriples[2] = fixedTriples[2];
    }

    public static long thisIsVariable(long varCode) {
        return -varCode;
    }

    public void setTriples(long[] triples) {
        this.triples = triples;
    }

    public long[] getTriples() {
        return triples;
    }

    public void setVariable(int index){
        triples[index] = thisIsVariable(triples[index]);
    }

    public void findStringTriple(HashMap<Long,String> reverseDictionary) {
        this.stringTriple[0] = reverseDictionary.get(triples[0]);
        this.stringTriple[1] = reverseDictionary.get(triples[1]);
        this.stringTriple[2] = reverseDictionary.get(triples[2]);
    }

    public static boolean isVariable(long code) {
        if(code < 0)
            return true;
        return  false;
    }

    public boolean matches(Triple triple) {
        if(triple.triples[0] != triples[0] && !isVariable(triples[0])  )
            return false;
        if(triple.triples[1] != triples[1] && !isVariable(triples[1])  )
            return false;
        if(triple.triples[2] != triples[2] && !isVariable(triples[2])  )
            return false;
        return true;
    }
}
