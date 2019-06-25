package triple;

import index.MyHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class TriplePattern2 {
    public final static int thisIsVariable = -1;

    private long triples[] = new long[3];
    public String stringTriple[] = new String[3];
    public long fixedTriples[] = new long[3];
    public HashMap<Long, Integer> variablesIndex;
    public String tempID; //for debug purpose only

    private Triple triple;

    private List<Triple> result;
    private ArrayList<TriplePattern2> rights;
    private ArrayList<TriplePattern2> lefts;

    private MyHashMap<Long, ArrayList<Triple>> Pso;
    private MyHashMap<String, ArrayList<Triple>> OPs;
    private MyHashMap<String, ArrayList<Triple>> SPo;

    //int varaibles[] = new int[3];

    public TriplePattern2(long s, long p, long o) {

        triple = new Triple(s, p, o);
    }

    public TriplePattern2(TriplePattern triplePattern) {
        triples[0] = triplePattern.triples[0];
        triples[1] = triplePattern.triples[0];
        triples[2] = triplePattern.triples[0];
        fixedTriples[0] = fixedTriples[0];
        fixedTriples[1] = fixedTriples[1];
        fixedTriples[2] = fixedTriples[2];
    }

    public void setIndexes(MyHashMap<Long, ArrayList<Triple>> Pso, MyHashMap<String, ArrayList<Triple>> OPs,
                           MyHashMap<String, ArrayList<Triple>> SPo) {
        this.Pso = Pso;
        this.OPs = OPs;
        this.SPo = SPo;
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

    public void setVariable(int index) {
        triples[index] = thisIsVariable(triples[index]);
    }

    public void findStringTriple(HashMap<Long, String> reverseDictionary) {
        this.stringTriple[0] = reverseDictionary.get(triples[0]);
        this.stringTriple[1] = reverseDictionary.get(triples[1]);
        this.stringTriple[2] = reverseDictionary.get(triples[2]);
    }

    public static boolean isVariable(long code) {
        if (code < 0)
            return true;
        return false;
    }

    public boolean connectTriplePattern(TriplePattern2 triplePattern, boolean right, boolean left) {
        if (rights == null) {
            rights = new ArrayList<TriplePattern2>();
            lefts = new ArrayList<TriplePattern2>();
        }
        if (right) {
            rights.add(triplePattern);
            return true;
        }
        if (left) {
            lefts.add(triplePattern);
            return true;
        }
        if (isVariable(triplePattern.triples[0]) && triplePattern.triples[0] == triples[0]) {
            lefts.add(triplePattern);
            return true;
        }
        if (isVariable(triplePattern.triples[2]) && triplePattern.triples[2] == triples[2]) {
            rights.add(triplePattern);
            return true;
        }
        return false;
    }

    public boolean matches(Triple triple) {
        if (triple.triples[0] != triples[0] && !isVariable(triples[0]))
            return false;
        if (triple.triples[1] != triples[1] && !isVariable(triples[1]))
            return false;
        if (triple.triples[2] != triples[2] && !isVariable(triples[2]))
            return false;
        return true;
    }


    private boolean evaluatedStarted = false;

    public void evaluatePatternHash() {
        if (result == null)
            result = new LinkedList<Triple>();
        if (!evaluatedStarted) {
            //try to get results from right
            TriplePattern2 rPattern = getJoinPattern(true);
            TriplePattern2 lPattern = getJoinPattern(false);
            if (rPattern != null)
                lPattern = null;
            // LinkedList<Triple> left = getJoinPatternLeft().getResult();
               /* if(rPattern != null && lPattern == null){
                    mergeJoin();
                    return;
                }*/
            hashJoin(rPattern, lPattern);
        } else {
            //TODO nothing to do here?
        }

        evaluatedStarted = true;
    }

    private void mergeJoin() {
        //TODO
    }

    private TriplePattern2 getJoinPattern(boolean right) {xx
        ArrayList<TriplePattern2> list = rights;
        if (!right)
            list = lefts;
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).isStarted())
                return list.get(i);
        }
        return null;
    }

    private void hashJoin(TriplePattern2 lPattern, TriplePattern2 rPattern) {

        if (isVariable(triples[1])) {
            System.out.println(" the hash index supports only constant properties");
            return;
        }
        if (SPo == null) {
            System.err.println("hash index requires setting indexes");
            return;
        }
        TriplePattern2 pattern = rPattern;
        int hisIndex = 0;
        MyHashMap<String, ArrayList<Triple>> index = OPs;
        if (lPattern == null && rPattern == null) {
            if (!isVariable(triples[0])) {
                index = SPo;
                List<Triple> list = index.get(triples[0], triples[1]);
                if (list != null && list.size() > 0)
                    result = list;
                evaluatedStarted = true;
                return;
            }

            if (!isVariable(triples[0])) {
                index = OPs;
                List<Triple> list = index.get(triples[2], triples[1]);
                if (list != null && list.size() > 0)
                    result = list;
                evaluatedStarted = true;
                return;
            }

        }
        if (lPattern == null) {
            pattern = lPattern;
            hisIndex = getHisJoinIndex(pattern);
            index = SPo;
        }
        List<Triple> right = pattern.getResult();
        //use the SPs
        // TriplePattern2 lJPattern = getJoinPatternLeft(pattern);
        //  if(lJPattern == null){
        //TODO we are done in the left direction
        //    return;
        //}
        for (int i = 0; i < right.size(); i++) {
            Triple rTriple = right.get(i);
            long val = rTriple.triples[hisIndex];
            if (val == 0)
                continue;
            List<Triple> list = index.get(val);
            if (list != null && list.size() > 0)
                result.addAll(list);
            else
                pattern.purne(rTriple, i, right.size());
        }
    }

    private void predicateEvaluate() {
        //Pso or Pos
        MyHashMap<Long, ArrayList<Triple>> index = Pso;
        result = index.get(triples[1]);
        evaluatedStarted = true;

    }

    private void purne(Triple rTriple, int index, int callerResultSize) {
        int mySize = lefts.size();
        if (evaluatedStarted && result.size() > index) {
            Triple triple = result.get(index);
            if (rTriple.triples[0] == rTriple.triples[0]
                    && rTriple.triples[1] == rTriple.triples[1] &&
                    rTriple.triples[2] == rTriple.triples[2])
                result.remove(index);
        } else {
            int strtIndex = mySize - index;
            if (callerResultSize > mySize) {
                strtIndex = index - (callerResultSize - mySize);
            } else {
                strtIndex = index + (mySize - callerResultSize);
            }
            for (int i = strtIndex; i < result.size(); i++) {
                Triple triple = result.get(i);
                if (rTriple.triples[0] == rTriple.triples[0]
                        && rTriple.triples[1] == rTriple.triples[1] &&
                        rTriple.triples[2] == rTriple.triples[2]) {
                    result.remove(index);
                    index = i;
                }
            }
        }

        for (int i = 0; i < lefts.size(); i++) {
            if (lefts.get(i).isStarted()) {
                lefts.get(i).purne(rTriple, index, result.size());
            }
        }
        for (int i = 0; i < rights.size(); i++) {
            if (rights.get(i).isStarted()) {
                rights.get(i).purne(rTriple, index, result.size());
            }
        }

    }

    private int getHisJoinIndex(TriplePattern2 triplePattern) {
        if (isVariable(triples[0]) && triples[0] == triplePattern.triples[0])
            return 0;
        if (isVariable(triples[2]) && triples[2] == triplePattern.triples[2])
            return 2;
        return -1;
    }

    private List<Triple> getResult() {
        return result;
    }

    private boolean isStarted() {
        return evaluatedStarted;
    }


}
