package triple;

import index.IndexesPool;
import index.MyHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class TriplePattern2 {
    public final static int thisIsVariable = -1;

    private int triples[] = new int[3];
    public String stringTriple[] = new String[3];
    public int fixedTriples[] = new int[3];
    public HashMap<Long, Integer> variablesIndex;
    public String tempID; //for debug purpose only



    private List<Triple> result;
    private ResultTriple resultTriple;
    private ArrayList<TriplePattern2> rights;
    private ArrayList<TriplePattern2> lefts;

    private MyHashMap<Integer, ArrayList<Triple>> Pso;
    private MyHashMap<Integer, ArrayList<Triple>> OPs;
    private MyHashMap<Integer, ArrayList<Triple>> SPo;

    private WithinIndex withinIndex ;

    //int varaibles[] = new int[3];

    /*public TriplePattern2(int s, int p, int o) {

        //triple = new Triple(s, p, o);
        withinIndex = new WithinIndex(0);
    }*/

    public TriplePattern2(TriplePattern triplePattern , IndexesPool indexesPool) {
        triples[0] = triplePattern.triples[0];
        triples[1] = triplePattern.triples[1];
        triples[2] = triplePattern.triples[2];
        fixedTriples[0] = fixedTriples[0];
        fixedTriples[1] = fixedTriples[1];
        fixedTriples[2] = fixedTriples[2];

        withinIndex = new WithinIndex(0);
        Pso = indexesPool.getIndex(IndexesPool.Pso);
        SPo = indexesPool.getIndex(IndexesPool.SPo);
        OPs = indexesPool.getIndex(IndexesPool.OPs);
    }


    public static int thisIsVariable(int varCode) {
        return -varCode;
    }

    public void setTriples(int[] triples) {
        this.triples = triples;
    }

    public int[] getTriples() {
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

    public static boolean isVariable(int code) {
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

    public void evaluatePatternHash(TriplePattern2 callerPattern) {
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
            hashJoin(callerPattern);
        } else {
            //TODO nothing to do here?
        }
        evaluatedStarted = true;
        TriplePattern2 next = getNextPattern();
        if(next != null)
            next.evaluatePatternHash(this);
    }

    private void mergeJoin() {
        //TODO
    }

    private TriplePattern2 getJoinPattern(boolean right) {
        ArrayList<TriplePattern2> list = rights;
        if (!right)
            list = lefts;
        if(list != null)
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).isStarted())
                return list.get(i);
        }
        return null;
    }


    private TriplePattern2 getNextPattern(){
        //look in left and right find the non started pattern with the minimum selectivty
        //assuming the  lists are already sorted for the  best  selectivity
        TriplePattern2 rPattern = null;
        for(int i = 0; rights !=null && i < rights.size() ; i++)
            if(!rights.get(i).isStarted())
                rPattern = rights.get(i);

        TriplePattern2 lPattern = null;
        for(int i = 0; lefts !=null && i < lefts.size() ; i++)
            if(!lefts.get(i).isStarted())
                lPattern = lefts.get(i);

        if(rPattern == null)
            return lPattern;
        if(lPattern == null)
            return rPattern;
        if(rPattern.getSelectivity() < lPattern.getSelectivity())
            return rPattern;
        return lPattern;

    }

    private void hashJoin(TriplePattern2 callerPattern) {

        if (isVariable(triples[1])) {
            System.out.println(" the hash index supports only constant properties");
            return;
        }
        if (SPo == null) {
            System.err.println("hash index requires setting indexes");
            return;
        }

        int hisIndex = 0;
        MyHashMap<Integer, ArrayList<Triple>> index = OPs;


        if (callerPattern == null) {
            if (!isVariable(triples[0])) {
                index = SPo;
                withinIndex.index = 0;
                List<Triple> list = index.get(triples[0], triples[1] , 1,withinIndex);
                if (list != null && list.size() > 0)
                    result = list;
                evaluatedStarted = true;
                return;
            }
            if (!isVariable(triples[2])) {
                index = OPs;
                withinIndex.index = 0;
                List<Triple> list = index.get(triples[2], triples[1],1,withinIndex);
                result.clear();
                if (list != null && list.size() > 0)
                    result.addAll(withinIndex.index ,list);
                evaluatedStarted = true;
                return;
            }

            predicateEvaluate();
            return;
        }


        if(isVariable(triples[0])){
            if(triples[0] == callerPattern.triples[2]){
                index = SPo;
                hisIndex = 2;
            }else if(triples[0] == callerPattern.triples[0]){
                index = SPo;
                hisIndex = 0;
            }
        }else if(isVariable(triples[2])) {
            if (triples[2] == callerPattern.triples[0]) {
                index = OPs;
                hisIndex = 0;
            } else if (triples[2] == callerPattern.triples[2]) {
                index = SPo;
                hisIndex = 2;
            }
        }
        List<Triple> hisRes = callerPattern.getResult();
        for (int i = 0; i < hisRes.size(); i++) {
            Triple hisTriple = hisRes.get(i);
            int hisVal = hisTriple.triples[hisIndex];
            int p = triples[1];
            if (hisVal == 0)
                continue;
            List<Triple> list = index.get(hisVal,p,1,withinIndex);
            if (list != null && list.size() > 0)
            for(int j= withinIndex.index ; j<list.size() ; j++) {
                Triple t = list.get(j);
                if(t.triples[1] != p)
                    break;
                result.add(t);
            }
            else
                callerPattern.purne(hisTriple, i, hisRes.size());
        }
    }

    private void predicateEvaluate() {
        //Pso or Pos
        MyHashMap<Integer, ArrayList<Triple>> index = Pso;
        result = index.get(triples[1]);
        evaluatedStarted = true;

    }

    private void purne(Triple rTriple, int index, int callerResultSize) {
        int mySize = lefts.size();
        if (evaluatedStarted && result.size() > index) {
            Triple triple = result.get(index);
            if (rTriple.triples[0] == triple.triples[0]
                    && rTriple.triples[1] == triple.triples[1] &&
                    rTriple.triples[2] == triple.triples[2])
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

    public List<Triple> getResult() {
        return result;
    }

    public boolean isStarted() {
        return evaluatedStarted;
    }


    public int getSelectivity() {
        return Triple.extractPredicateSelectivity(triples[1]);
    }


    public class WithinIndex{
        public int index ;
        public WithinIndex(int index){
            this.index = index;
        }
    }
}
