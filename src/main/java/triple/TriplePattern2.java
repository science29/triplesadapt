package triple;

import QueryStuff.ExecutersPool;
import QueryStuff.QueryExecuter;
import index.IndexesPool;
import index.MyHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

public class TriplePattern2 {
    public final static int thisIsVariable = -1;

    private int triples[] ;
   // public String stringTriple[] = new String[3];
   // public int fixedTriples[] = new int[3];
    public HashMap<Long, Integer> variablesIndex;
    public String tempID; //for debug purpose only


    //private List<Triple> result;
    private ResultTriple resultTriple;
    private ResultTriple headResultTriple;
    public int resultTripleShifLeft = 0;
    public int resultTripleShifRight = 0;
    private ArrayList<TriplePattern2> rights;
    private ArrayList<TriplePattern2> lefts;

    private MyHashMap<Integer, ArrayList<Triple>> Pso;
    private MyHashMap<Integer, ArrayList<Triple>> OPs;
    private MyHashMap<Integer, ArrayList<Triple>> SPo;

    private WithinIndex withinIndex;
    private int tempResultCnt = 0;
    private boolean goingLeft;
    private ExecutersPool executerPool;

    //int varaibles[] = new int[3];

    /*public TriplePattern2(int s, int p, int o) {

        //triple = new Triple(s, p, o);
        withinIndex = new WithinIndex(0);
    }*/

    public TriplePattern2(TriplePattern triplePattern, IndexesPool indexesPool) {
        triples = new int[3];
        triples[0] = triplePattern.triples[0];
        triples[1] = triplePattern.triples[1];
        triples[2] = triplePattern.triples[2];
        //fixedTriples[0] = fixedTriples[0];
        //fixedTriples[1] = fixedTriples[1];
       // fixedTriples[2] = fixedTriples[2];

        withinIndex = new WithinIndex(0);
        Pso = indexesPool.getIndex(IndexesPool.Pso);
        SPo = indexesPool.getIndex(IndexesPool.SPo);
        OPs = indexesPool.getIndex(IndexesPool.OPs);
    }


    private TriplePattern2(TriplePattern2 triplePattern){
        this.triples = triplePattern.triples;
        this.Pso = triplePattern.Pso;
        this.SPo = triplePattern.SPo;
        this.OPs = triplePattern.OPs;
        withinIndex = new WithinIndex(0);
    }

    public static TriplePattern2 getThreadReadyCopy(TriplePattern2 triplePattern) {
        TriplePattern2 triplePatternCopy = new TriplePattern2(triplePattern);
        return triplePatternCopy;
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

    /*public void findStringTriple(HashMap<Long, String> reverseDictionary) {
        this.stringTriple[0] = reverseDictionary.get(triples[0]);
        this.stringTriple[1] = reverseDictionary.get(triples[1]);
        this.stringTriple[2] = reverseDictionary.get(triples[2]);
    }*/

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


    public void setExecutorPool(ExecutersPool executorPool){
        this.executerPool = executorPool;
    }

    private boolean evaluatedStarted = false;

    public ResultTriple evaluatePatternHash(TriplePattern2 callerPattern , boolean deep) {
        // if (result == null)
        //     result = new LinkedList<Triple>();
        if (!evaluatedStarted) {
            //try to get results from right
            TriplePattern2 rPattern = getJoinPattern(true);
            TriplePattern2 lPattern = getJoinPattern(false);
            if (rPattern != null)
                lPattern = null;
            if (callerPattern == null && rPattern != null && rPattern.isStarted())
                callerPattern = rPattern;
            // LinkedList<Triple> left = getJoinPatternLeft().getResult();
               /* if(rPattern != null && lPattern == null){
                    mergeJoin();
                    return;
                }*/
            //   System.out.println("hash join triple:"+triples[0]+" "+triples[1]+" "+triples[2]);
            hashJoin(callerPattern , deep);
            if(deep)
                return headResultTriple;
        } else {
            //TODO nothing to do here?
        }
        evaluatedStarted = true;
        while (true) {
            TriplePattern2 next = getNextPattern();
            if (next == null)
                break;
            headResultTriple = next.evaluatePatternHash(this , deep);
            if (next.resultTripleShifLeft != 0) {
                if (!goingLeft) {
                    resultTripleShifLeft += (next.resultTripleShifLeft + 1);
                    resultTripleShifRight = 0;
                } else {
                    resultTripleShifRight += (next.resultTripleShifLeft + 1);
                    resultTripleShifLeft = 0;
                }
            } else {
                if (!goingLeft) {
                    resultTripleShifLeft += (next.resultTripleShifRight + 1);
                    resultTripleShifRight = 0;
                } else {
                    resultTripleShifRight += (next.resultTripleShifRight + 1);
                    resultTripleShifLeft = 0;
                }
            }
        }
        return headResultTriple;

    }

    private void mergeJoin() {
        //TODO
    }

    private TriplePattern2 getJoinPattern(boolean right) {
        ArrayList<TriplePattern2> list = rights;
        if (!right)
            list = lefts;
        if (list != null)
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).isStarted())
                    return list.get(i);
            }
        return null;
    }


    private TriplePattern2 getNextPattern() {
        //look in left and right find the non started pattern with the minimum selectivty
        //assuming the  lists are already sorted for the  best  selectivity
        TriplePattern2 rPattern = null;
        for (int i = 0; rights != null && i < rights.size(); i++)
            if (!rights.get(i).isStarted())
                rPattern = rights.get(i);

        TriplePattern2 lPattern = null;
        for (int i = 0; lefts != null && i < lefts.size(); i++)
            if (!lefts.get(i).isStarted())
                lPattern = lefts.get(i);

        goingLeft = true;
        if (rPattern == null)
            return lPattern;
        if (lPattern == null) {
            goingLeft = false;
            return rPattern;
        }
        if (rPattern.getSelectivity() < lPattern.getSelectivity()) {
            goingLeft = false;
            return rPattern;
        }
        return lPattern;

    }

    private void hashJoin(TriplePattern2 callerPattern , boolean deep) {

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
                List<Triple> list = index.get(triples[0], triples[1], 1, withinIndex);
                if (list != null && list.size() > 0) {
                    //     result = list;
                    headResultTriple = new ResultTriple(list);
                    resultTriple = headResultTriple;
                }
                evaluatedStarted = true;
                return;
            }
            if (!isVariable(triples[2])) {
                index = OPs;
                withinIndex.index = 0;
                List<Triple> list = index.get(triples[2], triples[1], 1, withinIndex);
                //  result.clear();
                if (list != null && list.size() > 0) {
                    //     result.addAll(withinIndex.index, list);
                    headResultTriple = new ResultTriple(list);
                    resultTriple = headResultTriple;
                }/*else
                    System.out.println("no result here");*/
                evaluatedStarted = true;
                return;
            }
            ArrayList<Triple> list = predicateEvaluate(!deep);
            if(deep) {
                if(executerPool == null)
                    startDeepEvaluation(list);
                //startDeepEvaluationParallel(list);
                else
                    startPoolDeepEvaluationParallel(list);
            }
            return;
        }

        if (isVariable(triples[0])) {
            if (triples[0] == callerPattern.triples[2]) {
                index = SPo;
                hisIndex = 2;
            } else if (triples[0] == callerPattern.triples[0]) {
                index = SPo;
                hisIndex = 0;
            }
        } else if (isVariable(triples[2])) {
            if (triples[2] == callerPattern.triples[0]) {
                index = OPs;
                hisIndex = 0;
            } else if (triples[2] == callerPattern.triples[2]) {
                index = SPo;
                hisIndex = 2;
            }
        }



        ResultTriple hisResultTriple = callerPattern.getHeadResultTriple();
        while (hisResultTriple != null) {
            Triple hisTriple = callerPattern.getShiftedResultPattern(hisResultTriple);
            int hisVal = hisTriple.triples[hisIndex];
            int p = triples[1];
            if (hisVal == 0)
                continue;
            List<Triple> list = index.get(hisVal, p, 1, withinIndex);
            if (list != null && list.size() > 0) {
                if (headResultTriple == null) {
                    headResultTriple = new ResultTriple(list.get(withinIndex.index));
                    resultTriple = headResultTriple;
                    if (hisIndex == 2)
                        resultTriple.left = hisResultTriple;
                    else
                        resultTriple.right = hisResultTriple;
                    withinIndex.index++;
                    tempResultCnt++;
                }
                for (int j = withinIndex.index; j < list.size(); j++) {
                    Triple t = list.get(j);
                    if (t.triples[1] != p)
                        break;
                    //result.add(t);
                    resultTriple.down = new ResultTriple(t);
                    resultTriple = resultTriple.down;
                    if (hisIndex == 2)
                        resultTriple.left = hisResultTriple;
                    else
                        resultTriple.right = hisResultTriple;
                    tempResultCnt++;
                }
            }
            //      else
            //       callerPattern.purne(hisTriple, i, hisRes.size());
            hisResultTriple = hisResultTriple.down;
        }
        /*if (resultTriple == null||  headResultTriple == null)
            System.out.println("no result here");
        else
            System.out.println("result size "+tempResultCnt);*/

    }



    private void startDeepEvaluation(ArrayList<Triple> list) {
        for(int j = 0 ; j < list.size() ; j++) {
            ResultTriple resultTriple = joinLeftRigth(list.get(j), this);
            if (resultTriple != null)
                connectResultTriple(resultTriple);
        }
    }

    ResultTriple finalReslut = null , pointerC = null;
    private synchronized void connectResultTriple(ResultTriple newOne){
        if(newOne != null) {
            if (finalReslut == null) {
                pointerC = newOne;
                finalReslut = newOne;
                headResultTriple =  finalReslut;
            }
            else {
                pointerC.down = newOne;
                pointerC = newOne;
            }
        }
    }


    public void startDeepEvaluationParallel(ArrayList<Triple> list){
        Stream<Triple> stream = list.stream();
        stream.parallel().forEach(triple -> {
            ResultTriple resultTriple = joinLeftRigth(triple , this);
            if(resultTriple != null){
                connectResultTriple(resultTriple);
            }
        });
    }

    /**
     * called form consumer produce thread
     * @param list
     * @param from
     * @param to
     */
    public void startWorkerDeepEvaluationParallel(ArrayList<Triple> list, int from, int to) {
        for(int i = from ; i <= to ; i++){
            ResultTriple resultTriple = joinLeftRigth(list.get(i) , this);
            if(resultTriple != null){
                connectResultTriple(resultTriple);
            }
        }
    }

    public void startPoolDeepEvaluationParallel(ArrayList<Triple> list){
        int from = 0 , to = 0;
        ArrayList<QueryExecuter> threadsList = executerPool.getThreadPool();
        int step =  list.size() / threadsList.size() ;
        for(int i = 0 ; i < threadsList.size() ; i++){
            to = from + step;
            if(to >= list.size())
                to = list.size() -1;
            threadsList.get(i).addWork(this, list, from, to, new QueryExecuter.CompleteListener() {
                @Override
                public void onComplete() {
                    workerDone(threadsList.size());
                }
            });
            from = to +1;
        }
    }


    private int doneCount = 0;
    private synchronized  void workerDone(int total){
       // System.out.println("Thread is done !");
        doneCount++;
        if(doneCount >= total)
            executerPool.finalListener.onComplete();
    }





    private ResultTriple hashJoinDeep(TriplePattern2 callPattern, Triple hisTriple , int hisIndex , MyHashMap<Integer, ArrayList<Triple>> index){
        boolean border = isBorder( hisTriple ,  hisIndex);
        int hisVal = hisTriple.triples[hisIndex];
        int p = triples[1];
        if (hisVal == 0)
            return null;
        WithinIndex withinIndex = new WithinIndex(-1);
        List<Triple> list = index.get(hisVal, p, 1, withinIndex);
        if (list != null && list.size() > 0) {
            ResultTriple myHeadResultTriple = null , myPointer = null;
            for (int j = withinIndex.index; j < list.size(); j++) {
                Triple t = list.get(j);
                if (t.triples[1] != p)
                    break;
                ResultTriple myResultTriple = joinLeftRigth(t,callPattern );
                if(myHeadResultTriple == null) {
                    myHeadResultTriple = myResultTriple;
                    myPointer = myResultTriple;
                }
                else{
                    myPointer.down = myResultTriple;
                    myPointer = myPointer.down;
                }
                if(border)
                    myResultTriple.setBorder();
                /*if(hisIndex == 0) {
                    hisResultTriple.left = myResultTriple
                    myResultTriple.right = hisResultTriple;
                }
                else{
                    hisResultTriple.right = myResultTriple;
                    myResultTriple.left = hisResultTriple;
                }*/
            }
            return myHeadResultTriple;
        }else {
            if(border)
                return ResultTriple.getDummyBorder();
            return null;
        }

    }

    private boolean isBorder(Triple hisTriple , int hisIndex){
        return  false;
        //TODO impliment this
    }



    private ResultTriple joinLeftRigth(Triple t , TriplePattern2 callPattern ){
        //result.add(t);
        ResultTriple myResultTriple = null;
        ResultTriple deepLeftTripleResult = null , deepRightTripleResult = null;
        // ResultTriple myResultTriple = new ResultTriple(t);
        ResultTriple headLeft = null , headRight = null;
        for(int i = 0; lefts != null && i < lefts.size() ; i++){
            TriplePattern2 pattern = lefts.get(i);
            if(pattern.equals(callPattern))
                continue;
            deepLeftTripleResult = pattern.hashJoinDeep(this , t ,0,OPs);
            if(deepLeftTripleResult == null)
                return null;
            if(myResultTriple == null)
                myResultTriple = new ResultTriple(t);
            if( i == 0) {
                myResultTriple.left = deepLeftTripleResult;
                headLeft = deepLeftTripleResult;
            }
            else {
                myResultTriple.left.down = deepLeftTripleResult;
                myResultTriple.left = myResultTriple.left.down;
            }
        }
        for(int i = 0; rights != null && i < rights.size() ; i++){
            TriplePattern2 pattern = rights.get(i);
            if(pattern.equals(callPattern))
                continue;
            deepRightTripleResult = pattern.hashJoinDeep(this , t ,2,SPo);
            if(deepRightTripleResult == null)
                return null;
            if(myResultTriple == null)
                myResultTriple = new ResultTriple(t);
            if( i == 0) {
                myResultTriple.right = deepRightTripleResult;
                headRight = deepRightTripleResult;
            }
            else {
                myResultTriple.right.down = deepRightTripleResult;
                myResultTriple.right = myResultTriple.right.down;
            }
        }
        if(myResultTriple == null)
            myResultTriple = new ResultTriple(t);
        myResultTriple.left = headLeft;
        myResultTriple.right = headRight;
        if(headLeft != null)
            headLeft.right = myResultTriple;
        return myResultTriple;
    }



    private Triple getShiftedResultPattern(ResultTriple hisResultTriple) {
        for (int i = 0; resultTripleShifLeft > 0 && i < resultTripleShifLeft; i++) {
            hisResultTriple = hisResultTriple.left;
        }

        for (int i = 0; resultTripleShifRight > 0 && i < resultTripleShifRight; i++) {
            hisResultTriple = hisResultTriple.right;
        }

        return hisResultTriple.triple;
    }


    private ArrayList<Triple> predicateEvaluate(boolean createHeadResult) {
        //Pso or Pos
        MyHashMap<Integer, ArrayList<Triple>> index = Pso;
        //result = index.get(triples[1]);
        ArrayList<Triple> list = index.get(triples[1]);
        if(createHeadResult) {
            headResultTriple = new ResultTriple(list);
            resultTriple = headResultTriple;
        }
        evaluatedStarted = true;
        return list;
    }


    private void purne(Triple rTriple, int index, int callerResultSize) {
        int mySize = lefts.size();
        /*if (evaluatedStarted && result.size() > index) {
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
        }*/

    }

    private int getHisJoinIndex(TriplePattern2 triplePattern) {
        if (isVariable(triples[0]) && triples[0] == triplePattern.triples[0])
            return 0;
        if (isVariable(triples[2]) && triples[2] == triplePattern.triples[2])
            return 2;
        return -1;
    }

   /* public List<Triple> getResult() {
        return result;
    }*/

    public boolean isStarted() {
        return evaluatedStarted;
    }


    public int getSelectivity() {
        int tempSelec = 0;
        if (isVariable(triples[0]))
            tempSelec++;
        if (isVariable(triples[2]))
            tempSelec++;
        if (!isVariable(triples[1]) && tempSelec > 1) {
            tempSelec++;
            tempSelec += Pso.get(triples[1]).size();
        }
        return tempSelec;
        //TODO:
        //  return Triple.extractPredicateSelectivity(triples[1]);
    }

    public ResultTriple getResultTriple() {
        return resultTriple;
    }

    public ResultTriple getHeadResultTriple() {
        return headResultTriple;
    }




    public class WithinIndex {
        public int index;

        public WithinIndex(int index) {
            this.index = index;
        }
    }

    public interface ExecuterCompleteListener{
        void onComplete();
    }
}
