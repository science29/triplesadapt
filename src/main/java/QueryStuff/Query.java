package QueryStuff;

import distiributed.SendItem;
import distiributed.Transporter;
import index.Dictionary;
import index.IndexesPool;
import optimizer.Optimiser;
import optimizer.Optimizer2;
import triple.ResultTriple;
import triple.Triple;
import triple.TriplePattern;
import triple.TriplePattern2;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Query {
    private static final boolean DEEP_PROCESSING = true ;
    private Transporter transporter;
    public ArrayList<TriplePattern> triplePatterns;
    public ArrayList<TriplePattern2> triplePatterns2;
    public int queryFrquency;
    public ArrayList<TriplePattern> simpleAnswer;
    public ArrayList<ArrayList<TriplePattern>> fullAnswer;

    private HashMap<TriplePattern, ArrayList<Triple>> answerMap;
    public String SPARQL;

    public String partitionString = "";
    public String answerString = "";

    private int startTime;

    boolean knownEmpty = false;

    private HashMap<String, Integer> varNameMap = new HashMap();

    private Dictionary dictionary ;
    private IndexesPool indexPool;
    private ArrayList<ResultTriple> results;
    private InterExecutersPool executersPool;

    public  int ID;
    private QueryWorkersPool queryWorkersPool;
    private int allowedThreadCount = 1;
    private boolean silent = false;
    private QueryCache queryCache;
    private QueryWorkersPool.Session batch;
    private Optimizer2 optimizer;
    private QueryDoneListener queryDoneListener;




    public interface QueryDoneListener{
        void done();
    }

    public Query(ArrayList<TriplePattern> triplePattern, int queryFrquency, ArrayList<TriplePattern> simpleAnswer) {
        this.ID = new Random().nextInt();
        this.triplePatterns = triplePattern;
        this.queryFrquency = queryFrquency;
        this.simpleAnswer = simpleAnswer;
    }



    public HashMap<TriplePattern, ArrayList<Triple>> getAnswerMap(){
        return answerMap;
    }


    public Query(Dictionary dictionary, String SPARQL , IndexesPool indexPool , Transporter transporter , Optimizer2 optimizer) {
        this.transporter = transporter;
        ID = new Random().nextInt();
        this.dictionary = dictionary;
        this.indexPool = indexPool;
        this.optimizer = optimizer;
        knownEmpty = !parseSparqlChain(SPARQL, dictionary);
    }

    public Query(Dictionary dictionary, String SPARQL , IndexesPool indexPool , Transporter transporter , int queryNo , Optimizer2 optimizer) {
        this.transporter = transporter;
        ID = queryNo;
        this.dictionary = dictionary;
        this.indexPool = indexPool;
        this.optimizer = optimizer;
        knownEmpty = !parseSparqlChain(SPARQL, dictionary);
    }

    public Query(Dictionary dictionary, TriplePattern2 first , IndexesPool indexPool , Transporter transporter , int queryNo , Optimizer2 optimizer) {
        this.transporter = transporter;
        ID = queryNo;
        this.dictionary = dictionary;
        this.indexPool = indexPool;
        this.optimizer = optimizer;
        addToTriplePatterns(first , true , null);
    }


    public void setDoneListener(QueryDoneListener queryDoneListener) {
        this.queryDoneListener = queryDoneListener;
    }

    public void done() {
        if(queryDoneListener != null)
            queryDoneListener.done();
    }

    public void findStringTriple(Dictionary reverseDictionary) {
        for (int i = 0; i < triplePatterns.size(); i++) {
            triplePatterns.get(i).findStringTriple(reverseDictionary);
        }
    }


    public void setQuerySPARQL(Dictionary reverseDictionary, HashMap<String, String> prefixIndex, HashMap<Integer, VertexGraph> verticies) {
        SPARQL = "";
        String predicates = "";
        ArrayList<String> varsList = new ArrayList();
        for (int i = 0; i < triplePatterns.size(); i++) {
            String s, p, o;
            if (triplePatterns.get(i).triples[0] != TriplePattern.thisIsVariable) {
                s = new String(reverseDictionary.get(triplePatterns.get(i).triples[0]));
                s = getFullStringElem(s, prefixIndex);
            } else {
                s = "?x" + -1 * triplePatterns.get(i).variablesIndex.get(triplePatterns.get(i).fixedTriples[0]);
                varsList.add(s);
            }
            if (triplePatterns.get(i).triples[1] != TriplePattern.thisIsVariable) {
                p = new String(reverseDictionary.get(triplePatterns.get(i).triples[1]));
                p = getFullStringElem(p, prefixIndex);
            } else
                p = "?x" + -1 * triplePatterns.get(i).variablesIndex.get(triplePatterns.get(i).fixedTriples[1]);
            if (triplePatterns.get(i).triples[2] != TriplePattern.thisIsVariable) {
                o = new String(reverseDictionary.get(triplePatterns.get(i).triples[2]));
                o = getFullStringElem(o, prefixIndex);
            } else
                o = "?x" + -1 * triplePatterns.get(i).variablesIndex.get(triplePatterns.get(i).fixedTriples[2]);

            predicates += s + " " + p + " " + o;
            if (i + 1 < triplePatterns.size())
                predicates += ".";
        }
        String vars = "";
        for (int k = 0; k < varsList.size(); k++) {
            vars = vars + " " + varsList.get(k);
        }
        SPARQL = "select " + vars + " where {" + predicates + "}";
        for (int i = 0; i < simpleAnswer.size(); i++) {
            int v = simpleAnswer.get(i).triples[0];
            answerString = answerString + "    " + (reverseDictionary.get(simpleAnswer.get(i).triples[0]));
            partitionString = partitionString + " " + verticies.get(v).partitionNumber;
        }
        // vars = "?x1";
    }

    private String getFullStringElem(String elem, HashMap<String, String> prefixIndex) {
        if (elem.contains("http"))
            return elem;
        if (!elem.contains("<") && elem.contains(":")) {
            String key = elem.split(":")[0];
            String pref = prefixIndex.get(key);
            String newPref = pref.replace(">", elem.split(":")[1].trim() + ">");
            return newPref;
        }
        if (elem.contains("<")) {
            String pref = prefixIndex.get("base");
            String elemT;
            elemT = elem.replace("<", "");
            elemT = elemT.replace(">", "");
            String newPref = pref.replace(">", elemT.trim() + ">");
            return newPref;
        }
        return elem;
    }

    public static ArrayList<Triple> join(ArrayList<Triple> ss, ArrayList<Triple> pp, ArrayList<Triple> oo) {
        ArrayList<Triple> r1 = null;
        ArrayList<Triple> r2 = null;
        int i1 = 0, i2 = 0;
        if (ss != null)
            r1 = ss;
        else if (pp != null) {
            r1 = pp;
            i1 = 1;
        } else {
            r1 = oo;
            i1 = 2;
            return r1;
        }

        if (ss != null) { // means : r1 = ss
            if (pp != null) {
                r2 = pp;
                i2 = 1;
            } else if (oo != null) {
                r2 = oo;
                i2 = 2;
            } else
                return r1;
        } else if (pp != null && oo != null) {
            r2 = oo;
            i2 = 2;
        }

        if (r2 == null || r1 == null)
            return r1;

        ArrayList res = join(r1, r2, i1, i2);

        return res;

    }


    public static ArrayList<Triple> join(ArrayList<Triple> t1, ArrayList<Triple> t2, int i1, int i2) {
        int match1 = t1.get(0).triples[i1];
        int match2 = t2.get(0).triples[i2];
        ArrayList<Triple> res = new ArrayList();
        for (int j = 0; j < t1.size(); j++) {
            if (t1.get(j).triples[i2] == match2) {
                res.add(t1.get(j));
            }
        }
        for (int j = 0; j < t2.size(); j++) {
            if (t2.get(j).triples[i1] == match1) {
                res.add(t2.get(j));
            }
        }

        return res;
    }


    public void findChainQueryAnswer(HashMap<String, ArrayList<Triple>> OPxP, HashMap<String, ArrayList<Triple>> opS , StringBuilder timeString) {
        if(knownEmpty)
            return;
        //HashMap<TriplePattern, Triple> answer = new HashMap();
        answerMap = new HashMap();
        //first find triple pattern that has po fixed
        TriplePattern triplePattern1 = null, triplePattern2 = null;
        for (int i = triplePatterns.size() - 1; i >= 0; i--) {
            triplePattern1 = triplePatterns.get(i);
            if (!TriplePattern.isVariable(triplePattern1.triples[1]) && !TriplePattern.isVariable(triplePattern1.triples[2]) && TriplePattern.isVariable(triplePattern1.triples[0]))
                break;
            triplePattern1 = null;
        }
        if (triplePattern1 == null) {
            System.err.println("Error finding po fixed in chain query");
            return;
        }
        processedTripelPattern(triplePattern1);
        //now look for the other p
        for (int i = triplePatterns.size() - 1; i >= 0; i--) {
            triplePattern2 = triplePatterns.get(i);
            if (triplePattern1.triples[0] == triplePattern2.triples[2])
                break;
            triplePattern2 = null;
        }
        if(triplePattern2 == null){
            //todo solve this
            return;
        }
        processedTripelPattern(triplePattern2);
        //now build the index key OPP
        String key = triplePattern1.triples[2] + "" + triplePattern1.triples[1] + "" + triplePattern2.triples[1];
        //TODO remove time stuff
        long startTime = System.nanoTime();

        ArrayList<Triple> list = OPxP.get(key);
        long stopTime = System.nanoTime();
        long OPxpElapsedTime = (stopTime - startTime) / 1000;
        timeString.append("time to opxp "+OPxpElapsedTime);

        if(list == null)
            list = temp(opS,triplePattern1.triples[2],triplePattern1.triples[1],triplePattern2.triples[1]);
        //now move to all triples in list
        TriplePattern nextTripelPatern = getNextTriplePattern(triplePattern2, 0);
        if(nextTripelPatern == null)
            return;
        for (int i = 0; i < list.size() - 1; i += 2) {
            Triple triple1 = list.get(i);
           // answer.put(triplePattern1, triple1);
            Triple Triple = list.get(i + 1);
           // answer.put(triplePattern2, Triple);
         //   int next_o = Triple.triples[0];

          //  int next_p = nextTripelPatern.triples[1];
          //  key = next_o + "" + next_p;
         //   ArrayList<Triple> list2 = opS.get(key);

            //addToWorkToThreads(opS,nextTripelPatern, triple1 ,Triple);
          boolean res =  findDeepAnswer(nextTripelPatern, Triple, opS);
          if(res){
              addToAnswerMap(triplePattern1, triple1);
              addToAnswerMap(triplePattern2, Triple);
          }
            //answer.put(nextTripelPatern, Triple);
        }
    //    noMoreWork();


    }



    public void findQueryAnswer(InterExecutersPool executersPool , QueryWorkersPool queryWorkersPool , int allowedThreadCount , TriplePattern2.ExecuterCompleteListener executerCompleteListener  , QueryCache queryCache){
        this.executersPool = executersPool;
        this.queryWorkersPool = queryWorkersPool;
        this.allowedThreadCount = allowedThreadCount;
        this.queryCache = queryCache;
        executersPool.setFinalListener(executerCompleteListener);
        findQueryAnswer();
    }

    public void findQueryAnswer(QueryWorkersPool queryWorkersPool){
        this.queryWorkersPool = queryWorkersPool;
        findQueryAnswer();
    }

    public ArrayList<ResultTriple> findQueryAnswer(){
      //find the triplePattern to start with
      //start executing and let it propogate.

        //looks first for cached results
        TriplePattern2 cachedPattern = triplePatterns2.get(0).setCachedResult(queryCache , null);
        if(cachedPattern != null){
            cachedPattern.startCachedProcessing(null);
        }
        Collections.sort(triplePatterns2, new Comparator<TriplePattern2>() {
            // @Override
            public int compare(TriplePattern2 lhs, TriplePattern2 rhs) {
                // -1 - less than, 1 - greater than, 0 - equal, all inversed for descending
                if(lhs.getSelectivity() > rhs.getSelectivity())
                    return 1;
                if(lhs.getSelectivity() < rhs.getSelectivity())
                    return -1;
                return 0;
                // return lhs.customInt > rhs.customInt ? -1 : (lhs.customInt < rhs.customInt) ? 1 : 0;
            }
        });
         results = new ArrayList<ResultTriple>();
         if(DEEP_PROCESSING) {
             triplePatterns2.get(0).setExecutorPool(executersPool , allowedThreadCount);
             results.add(triplePatterns2.get(0).evaluatePatternHash(null, true));
         }
         else
      for(int i =0 ; i < triplePatterns2.size() ; i++){
          if(!triplePatterns2.get(i).isStarted()) {
             // System.out.println("selectivity: "+triplePatterns2.get(i).getSelectivity());
              ResultTriple resultTriple = triplePatterns2.get(i).evaluatePatternHash(null , true);
              results.add(resultTriple);
          }
      }
        return results;
    }




    private int threadIndex = 0;
    QueryWorker [] workers = new QueryWorker[8];
    int threadCount = 2;
    private void addToWorkToThreads(HashMap<String, ArrayList<Triple>> Ops, TriplePattern triplePattern, Triple triple1, Triple Triple){
        if(threadIndex == 0 && workers[0] == null) {
            workers[0] = new QueryWorker(Ops);
            workers[0].start();
        }
        else if( workers[threadIndex] == null) {
            workers[threadIndex] = new QueryWorker(Ops, workers[0]);
            workers[threadIndex].start();
        }
        workers[threadIndex].addWork(triplePattern , triple1 , Triple);
        threadIndex++;
        if(threadIndex >= threadCount)
            threadIndex = 0;
    }

    private void noMoreWork(){
        for(int i=0 ; i <workers.length ; i++){
            if(workers[i] != null)
                workers[i].noMoreWork();
        }
    }

    private ArrayList<Triple> temp(HashMap<String, ArrayList<Triple>> opS , int o1 ,int p1 , int p2){
        ArrayList<Triple> res = new ArrayList<Triple>();
        ArrayList<Triple> list = opS.get(o1 + "" + p1);
        for(int i= 0; i < list.size() ; i++){
            int next_o = list.get(i).triples[0];
            ArrayList<Triple> list2 = opS.get(next_o + "" + p2);
            for(int j =0; j<list2.size() ; j++)
                res.add(list2.get(j));
        }
        return res;
    }

    @Deprecated
    private boolean findDeepAnswer(TriplePattern triplePattern, Triple triple, HashMap<String, ArrayList<Triple>> opS) {
        int next_o = triple.triples[0];
        int next_p = triplePattern.triples[1];
        String key = next_o + "" + next_p;
        ArrayList<Triple> list = opS.get(key);
        if (list == null)
            return false;
        TriplePattern nextTripelPatern = getNextTriplePattern(triplePattern, 0);
        boolean found = false;
        boolean matches;
        for (int i = 0; i < list.size(); i++) {
            Triple Triple = list.get(i);
            matches = triplePattern.matches(Triple);
            if(!matches)
                continue;
            boolean res = true;
            if (nextTripelPatern != null)
                res = findDeepAnswer(nextTripelPatern, Triple, opS);
            if (res) {
                found = true;
                addToAnswerMap(triplePattern, triple);
            }
        }
        return found;
    }

    @Deprecated
    private void addToAnswerMap(TriplePattern triplePattern, Triple triple) {
        if (answerMap == null)
            answerMap = new HashMap();
        ArrayList<Triple> answerList = answerMap.get(triplePattern);
        if (answerList == null) {
            answerList = new ArrayList();
            answerMap.put(triplePattern, answerList);
        }
        answerList.add(triple);
    }


    private void findNextTripleAnswer(Triple currentTriple, TriplePattern currTriplePattern, TriplePattern nextTriplePattern) {

    }


    private HashMap<TriplePattern, TriplePattern> seenPatterns = new HashMap();

    private void processedTripelPattern(TriplePattern triplePattern){
        seenPatterns.put(triplePattern,triplePattern);
    }
    private TriplePattern getNextTriplePattern(TriplePattern triplePattern, int index) {
        //find the triple pattern that is connecte to triplePattern from index i
        //find the index in triplePattern to find
        int searchIndex = 0 ;
        if(index == 0)
            searchIndex = 2;
        TriplePattern firstNotSeen = null;
        for (int i = 0; i < triplePatterns.size(); i++) {
            TriplePattern candiatePattern = triplePatterns.get(i);
            if (!seenPatterns.containsKey(candiatePattern)) {
                if (firstNotSeen == null)
                    firstNotSeen = candiatePattern;
                if (candiatePattern.triples[searchIndex] == triplePattern.triples[index]) {
                    seenPatterns.put(candiatePattern, candiatePattern);
                    return candiatePattern;
                }
            }

        }
        if(firstNotSeen != null)
            seenPatterns.put(firstNotSeen, firstNotSeen);
        return firstNotSeen;
    }


    public boolean parseSparqlChain(String spaql, index.Dictionary dictionary) {
        /*" select  ?x1 ?x3 ?x5 ?x7 where " +
                "{?x1 <http://mpii.de/yago/resource/describes> ?x3.?x3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?x5." +
                "?x5 <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?x7." +
                "?x7 <http://mpii.de/yago/resource/isPartOf> <http://mpii.de/yago/resource/wordnet_transportation_system_104473432>} ";*/
        triplePatterns = new ArrayList();
        triplePatterns2 = new ArrayList<TriplePattern2>();
        String [] proj = spaql.split(" ");
        String s = spaql.split("\\{")[1];
        // s = s.replace("}", "");
        boolean build = false, varStart = false;
        String last = "";
        Integer[] code = new Integer[3];
        boolean blackNode = false;
        boolean constantStart = false;
        boolean [] projectedFlags = new boolean[3];
        int index = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case ' ':
                    if(constantStart) {
                        last += " ";
                        break;
                    }
                    if( last.equals("") )
                        break;
                    if (varStart)
                        code[index++] = TriplePattern.thisIsVariable(getNunqieVarID(last));
                    if(blackNode) {
                        code[index++] = dictionary.get(last);
                        build = false;
                        blackNode = false;
                    }
                    varStart = false;
                    last = "";
                    break;
                case '}':
                    if (varStart)
                        code[index++] = TriplePattern.thisIsVariable(getNunqieVarID(last));
                    if(blackNode)
                        code[index++] = dictionary.get(last);
                    return addToTriplePatterns(code,projectedFlags);
                case '?':
                    if(!build) {
                        varStart = true;
                        last = "?";
                    }else {
                        last = last + c;
                    }
                    break;
                case '<':
                    build = true;
                    last = "<";
                    break;
                case '>':
                    build = false;
                    last += ">";
                    code[index++] = dictionary.get(last);
                    break;
                case '\"':
                    if(!build) {
                        constantStart = true;
                        build = true;
                        last = "\"";
                    }else{
                        constantStart = false;
                        last += "\"";
                        code[index++] = dictionary.get(last);
                    }
                    break;

                case '.':
                    if(build)
                        last = last + c;
                    if (!varStart)
                        break;
                    code[index++] = TriplePattern.thisIsVariable(getNunqieVarID(last));
                    varStart = false;
                    last = "";
                    boolean res = addToTriplePatterns(code,projectedFlags);
                    index = 0;
                    code[0] = (int)0;code[1] = (int)0;code[2] = (int)0;
                    if(!res)
                        return false;
                    break;

                case '_':
                    if (build || varStart)
                        last = last + c;
                    else if(i < s.length()+1 && s.charAt(i+1) == ':'){
                        build = true;
                        last = "_";
                        blackNode = true;
                    }
                    break;

                default:
                    if (build || varStart)
                        last = last + c;
                    else{//we look for prefix
                      String co = detectPrefix(s , i);
                      if(co != null) {
                          code[index++] = dictionary.get(co);
                          i += (co.length() -1);
                          break;
                      }
                    }
            }
        }
        System.err.println("error parsing query");
        return false;
        /*
        String[] patterns = s.split("\\.");
        for (int j = 0; j < patterns.length; j++) {
            String[] x = patterns[j].split(" ");

            int ss, pp, oo;
            try {
                if (x[0].startsWith("?"))
                    ss = TriplePattern.thisIsVariable(getNunqieVarID(x[0]));
                else
                    ss = dictionary.get(x[0]);
                if (x[1].startsWith("?"))
                    pp = TriplePattern.thisIsVariable(getNunqieVarID(x[1]));
                else
                    pp = dictionary.get(x[1]);
                if (x[2].startsWith("?"))
                    oo = TriplePattern.thisIsVariable(getNunqieVarID(x[2]));
                else
                    oo = dictionary.get(x[2]);
                TriplePattern triplePattern = new TriplePattern(ss, pp, oo);
                triplePatterns.add(triplePattern);
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("error parsing query");
                // System.exit(1);
            }
        }*/
    }


    private String detectPrefix(String s , int startIndex ){
        String temp = "";
        boolean prefixDetected = false;
        for(int j = startIndex ; j < s.length() ; j++ ){
            if(s.charAt(j) == ':')
                prefixDetected = true;
            if(s.charAt(j) == ' ' || s.charAt(j) == '}' || s.charAt(j) == '.') {
                if (prefixDetected)
                   return temp;
                else
                    return null;
            }
            temp += s.charAt(j);
        }
        return null;
    }


    public boolean addToTriplePatterns(Integer[] code , boolean[] projected){

        if (code[0] == null || code[1] == null || code[2] == null) {
            System.err.println("query answer is empty");
            return false;
        }
        TriplePattern triplePattern = new TriplePattern(code[0], code[1], code[2]);
        //TODO do this
///        triplePattern.setProjected(projected);
        triplePatterns.add(triplePattern);
        TriplePattern2 triplePattern2 = new TriplePattern2(triplePattern ,indexPool ,transporter,this , optimizer);
        connectTriplePatterns(triplePattern2);
        triplePatterns2.add(triplePattern2);


        return true;
    }

    public void addToTriplePatterns(TriplePattern2 triplePatternObj , boolean rolling , TriplePattern2 prevPattern){
        if(triplePatternObj == null)
            return;
        if(rolling && triplePatternObj.getLefts() != null && triplePatternObj.getLefts().size() > 0){
            addToTriplePatterns(triplePatternObj.getLefts().get(0) , true , null);
            return;
        }
        triplePatterns2.add(triplePatternObj);

        for(int i = 0 ; triplePatternObj.getRights() != null && i < triplePatternObj.getRights().size(); i++ ){
            addToTriplePatterns(triplePatternObj.getRights().get(i) , false , triplePatternObj);
        }


        for(int i = 0 ; triplePatternObj.getLefts() != null && i < triplePatternObj.getLefts().size(); i++ ){
            addToTriplePatterns(triplePatternObj.getLefts().get(i) , false , triplePatternObj);
        }

    }




    private void connectTriplePatterns(TriplePattern2 triplePattern){
        for(int i = 0 ; i < triplePatterns2.size() ; i++){
            if(triplePatterns2.get(i).getTriples()[0] == triplePattern.getTriples()[0] &&
                    TriplePattern2.isVariable(triplePattern.getTriples()[0])) {
                triplePatterns2.get(i).connectTriplePattern(triplePattern, false, true);
                triplePattern.connectTriplePattern(triplePatterns2.get(i), false, true);
            }
            if(triplePatterns2.get(i).getTriples()[2] == triplePattern.getTriples()[2] &&
                    TriplePattern2.isVariable(triplePattern.getTriples()[2])) {
                triplePatterns2.get(i).connectTriplePattern(triplePattern, true, false);
                triplePattern.connectTriplePattern(triplePatterns2.get(i), true, false);
            }


            if(triplePatterns2.get(i).getTriples()[0] == triplePattern.getTriples()[2] &&
                    TriplePattern2.isVariable(triplePattern.getTriples()[2])) {
                triplePatterns2.get(i).connectTriplePattern(triplePattern, false, true);
                triplePattern.connectTriplePattern(triplePatterns2.get(i), true, false);
            }

            if(triplePatterns2.get(i).getTriples()[2] == triplePattern.getTriples()[0] &&
                    TriplePattern2.isVariable(triplePattern.getTriples()[0])) {
                triplePatterns2.get(i).connectTriplePattern(triplePattern, true, false);
                triplePattern.connectTriplePattern(triplePatterns2.get(i), false, true);
            }

        }
    }

    int nextVarcode = 1;

    private int getNunqieVarID(String x) {
        Integer n = varNameMap.get(x);
        if (n == null) {
            n = nextVarcode++;
            varNameMap.put(x, n);
        }
        return n;
    }



    public ResultTriple getTripleResults() {
       return triplePatterns2.get(0).getHeadResultTriple();
    }


    boolean temp = true;


    public void printAnswers(Dictionary reverseDictionary ) {
        if(knownEmpty)
            return;
        results.remove(0);
        results.add(triplePatterns2.get(0).getHeadResultTriple());//TODO fix this so soon
        if(results != null){
            int count = 0;
            for(int i =0 ; i < results.size() ; i++){
                ResultTriple vResultTriple = results.get(i);
                if (vResultTriple == null)
                    break;
                do {
                    ResultTriple hResultTriple = vResultTriple.getFarLeft();
                    while (hResultTriple != null) {
                        ResultTriple pHTriple = hResultTriple;
                        boolean moreThanOne = false;
                        do {
                            Triple triple = pHTriple.getTriple();
                            if (!silent && triple != null) {
                                String str = reverseDictionary.getString(triple.triples[0]) + " " + reverseDictionary.getString(triple.triples[1]) + " " + reverseDictionary.getString(triple.triples[2]);//TODO consider calling get and process the char [] more effciently
                                System.out.print(str + " . ");
                            }
                            count = printExtra(pHTriple.getExtraDown() , reverseDictionary , count ,silent);
                            pHTriple = pHTriple.getDown();
                            if(moreThanOne) {
                                System.out.println();
                                count++;
                            }
                            moreThanOne = true;
                        }while(pHTriple != null && vResultTriple != hResultTriple);
                        hResultTriple = hResultTriple.getRight();
                    }
                    if(!silent)
                        System.out.println();
                    count++;
                    vResultTriple = vResultTriple.getDown();
                }while (vResultTriple != null);
            }
            System.out.println( "Total result size="+count);
        }



    /*    if(!temp)
            return;
        temp = false;
        SendItem sendItem = new SendItem(0 ,triplePatterns2.get(0).getTriples() ,triplePatterns2.get(0).getHeadResultTriple());
        //sendItem.fromByte(sendItem.getBytes());
        long start = System.nanoTime();
        transporter.sendToAll(sendItem);
        transporter.receive(0, new Transporter.ReceiverListener() {
            @Override
            public void gotResult(SendItem sendItem) {
                long end = System.nanoTime();
                System.err.println("got from remote took:"+(end-start)/1000 + " Ms ");
                triplePatterns2.get(0).headResultTriple = sendItem.resultTriple;
                printAnswers(reverseDictionary ,false);
            }
        });
*/
    }

    private int printExtra(ResultTriple pHTriple, Dictionary reverseDictionary, int count, boolean silent) {
        if(pHTriple == null)
            return count;
        Triple triple = pHTriple.getTriple();
        if (!silent && triple != null) {
            String str = reverseDictionary.get(triple.triples[0]) + " " + reverseDictionary.get(triple.triples[1]) + " " + reverseDictionary.get(triple.triples[2]);
            System.out.println(str + " . ");
        }
        count++;
        return printExtra(pHTriple.getExtraDown() , reverseDictionary , count , silent);
    }

    private void qeuryDone() {
        long stopTime = System.nanoTime();
        long elapsedTime = (stopTime - startTime) / 1000;
        System.out.println("query time threads :"+elapsedTime);
        printAnswers(dictionary);
    }

    public static boolean sep() {
        int ch = new Random().nextInt(100) + 1;
        if(ch < 76)
            return true;
        return false;
    }

    public boolean isPendingBorder() {
        for(int i = 0 ; i < triplePatterns.size() ; i++){
            if(triplePatterns2.get(i).pendingBorder)
                return true;
        }
        return false;
    }

    public void borderEvaluation() {
        //triplePatterns2.get(0).rightLeftBorderEvaluation(triplePatterns2.get(0));
        triplePatterns2.get(0).rightLeftBorderEvaluation2Start();
    }



    public void gotRemoteResult(SendItem sendItem) {
        triplePatterns2.get(0).gotRemoteBorderResult(sendItem);
        if(queryWorkersPool != null)
            queryWorkersPool.moveFormPendingToWorking(sendItem.queryNo);
    }

    public SendItem getToSendItem() {
        if(triplePatterns2.get(0).getHeadTempBorderList() == null)
            return null;
        SendItem sendItem = new SendItem(ID ,triplePatterns2.get(0).getTriples() , triplePatterns2.get(0).getHeadTempBorderList());
        return sendItem;
    }

    public void setSilent(boolean status) {
        silent = status;
    }

    public static ArrayList<Integer> getQueiresNumber(int size) {
        Random ran = new Random();
        ArrayList<Integer> nos = new ArrayList<>();
        for(int i = 0; i < size ; i++)
            nos.add(ran.nextInt());
        return nos;
    }

    public void setBatch(QueryWorkersPool.Session batch) {
        this.batch = batch;
    }

    public QueryWorkersPool.Session getBatch() {
        return batch;
    }


    @Deprecated
    class QueryWorker extends Thread /*implements Runnable*/{
        private final HashMap<String, ArrayList<Triple>> opS;
        private Query.QueryWorker mainWorker;
        public BlockingQueue<Triple> sharedTriple1Queue;
        public BlockingQueue<Triple> sharedTripleQueue;
        public  BlockingQueue<TriplePattern> sharedPatternQueue;
        private boolean stop = false;
        private int doneCount;
        private int threadCount;


        public QueryWorker( HashMap<String, ArrayList<Triple>> opS){
            this.opS = opS;
            this.sharedPatternQueue =  new LinkedBlockingQueue<TriplePattern>();
            this.sharedTriple1Queue = new LinkedBlockingQueue<Triple>();
            this.sharedTripleQueue = new LinkedBlockingQueue<Triple>();
            this.mainWorker = this;
        } 

        public QueryWorker( HashMap<String, ArrayList<Triple>> opS , QueryWorker mainWorker) {
            this.opS = opS;
            /*this.sharedPatternQueue = mainWorker.sharedPatternQueue;
            this.sharedTriple1Queue = mainWorker.sharedTriple1Queue;
            this.sharedTripleQueue = mainWorker.sharedTripleQueue;*/

            this.sharedPatternQueue =  new LinkedBlockingQueue<TriplePattern>();
            this.sharedTriple1Queue = new LinkedBlockingQueue<Triple>();
            this.sharedTripleQueue = new LinkedBlockingQueue<Triple>();
            this.mainWorker = mainWorker;
        }
        
        public synchronized void imDone(){
            doneCount++;
            if(doneCount == threadCount)
                qeuryDone();
        }
        

        public void noMoreWork(){
            Triple triple = new Triple(0,0,0);
            if(sharedPatternQueue != null)
                sharedPatternQueue.add(new TriplePattern(0,0,0) );
            sharedTriple1Queue.add(triple);
            sharedTripleQueue.add(triple );
        }

        public int getBufferSize(){
            return sharedPatternQueue.size();
        }

        public void stopThread(){
            stop = true;
            Triple triple = new Triple(0,0,0);
            if(sharedPatternQueue != null)
                sharedPatternQueue.add(new TriplePattern(0,0,0) );
            sharedTriple1Queue.add(triple);
            sharedTripleQueue.add(triple );
        }

        public void addWork(TriplePattern triplePattern , Triple triple1 , Triple Triple) {
            try {
                sharedPatternQueue.put(triplePattern);
                sharedTriple1Queue.put(triple1);
                sharedTripleQueue.put(Triple);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        public void run() {
            while(!stop){
                try {
                    TriplePattern triplePattern = sharedPatternQueue.take();
                    Triple triple1 = sharedTriple1Queue.take();
                    Triple Triple = sharedTripleQueue.take();
                    if(triple1.triples[0] == 0 && mainWorker != null) {
                        mainWorker.imDone();
                        return;
                    }
                    boolean res = findDeepAnswer(triplePattern, Triple, opS);
                    if(res && !stop) {
                        addToAnswerMap(triplePattern, triple1);
                        addToAnswerMap(triplePattern, Triple);
                    }
                    
                    //System.out.println("Consumed: "+ num + ":by thread:"+threadNo);
                } catch (Exception err) {
                    err.printStackTrace();
                }
            }
        }
    }



}
