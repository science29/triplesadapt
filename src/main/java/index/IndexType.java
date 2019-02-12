package index;

import triple.TriplePattern;

public class IndexType {


    public final int [] keyType ;

    public IndexType(int s,int p ,int o){
        int [] t = new int [3];
        t[0] = s;
        t[1] = p;
        t[2] = o;
        this.keyType = t;
    }
    public IndexType(int[] keyType) {
        if(keyType == null) {
            int [] keyTypet = {0, 0, 0};
            this.keyType = keyTypet;
            return;
        }
        this.keyType = keyType;
    }

    public IndexType(){
        int [] keyTypeTT = {0, 0, 0};
        this.keyType = keyTypeTT;
    }


    public IndexType(TriplePattern triplePattern) {
        long sub = triplePattern.triples[0];
        long pro = triplePattern.triples[1];
        long obj = triplePattern.triples[2];
        int s,o,p;
        if(!TriplePattern.isVariable(sub))
            s = 1;
        else
            s = 0;

        if(!TriplePattern.isVariable(pro))
            p  = 1;
        else
            p = 0;

        if(!TriplePattern.isVariable(obj))
            o = 1;
        else
            o = 0;
        keyType = new int [3];
        keyType[0] =  s; keyType[1] =  p; keyType[2] =  o;
    }

    public Integer getCode() {
        int code = keyType[2]*100 + keyType[1]*10 +keyType[0];
        return code;
    }

    public static Integer getCode(TriplePattern triplePattern){
        long sub = triplePattern.triples[0];
        long pro = triplePattern.triples[1];
        long obj = triplePattern.triples[2];

        if(!TriplePattern.isVariable(sub))
            sub = 100;
        else
            sub = 0;

        if(!TriplePattern.isVariable(pro))
            pro  = 10;
        else
            pro = 0;

        if(!TriplePattern.isVariable(obj))
            obj = 1;
        else
            obj = 0;
        return (int)(sub+pro+obj);

    }
}
