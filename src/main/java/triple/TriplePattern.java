package triple;

import index.Dictionary;

import java.util.HashMap;


public class TriplePattern {
    public final static int thisIsVariable = -1;
    public int triples[]= new int[3];
    public String stringTriple[] = new String[3] ;
    public int fixedTriples[] = new int[3];
    public HashMap<Integer , Integer> variablesIndex;
    public String tempID ; //for debug purpose only
    public boolean pendingBorder = false;


    //int varaibles[] = new int[3];

    public TriplePattern(int s ,int p , int o){
        triples[0] = s ;
        triples[1] = p;
        triples[2] = o;
        fixedTriples[0] = s ;
        fixedTriples[1] = p;
        fixedTriples[2] = o;
    }

    public static int thisIsVariable(int varCode) {
        return -varCode;
    }


    public void findStringTriple(Dictionary reverseDictionary) {
        this.stringTriple[0] = reverseDictionary.get(triples[0]);
        this.stringTriple[1] = reverseDictionary.get(triples[1]);
        this.stringTriple[2] = reverseDictionary.get(triples[2]);
    }

    public static boolean isVariable(int code) {
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
