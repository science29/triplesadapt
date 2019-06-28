package QueryStuff;

import QueryStuff.Query;
import index.Dictionary;
import index.MyHashMap;
import start.MainUinAdapt;
import triple.Triple;
import triple.TriplePattern;

import java.util.*;

public class QueryGenrator {

    /**
     * for each query we have the following parametes :
     * 1 the number of triple patterns
     * 2 the degree of freedom in each triple patrren
     * 3 the frequency
     *
     */

    /**
     * for each triple pattern we have
     *
     * 1 the number of variables (degree of freedom)
     * 2 the
     */

    /**
     * gererate random query grpah:
     * 1 The number of nodes per graph is n
     * 2 the max number of edges per node is k
     * 3 Randomly pick a starting node v from V
     * 4 in v, randomly choose the number of edges e (max is k)
     * 5 randomly choose e edges (v,vi) 0<i<e.
     * 6 for each of (v,vi) edges , repeat from 3 puting v=vi until the visited node = n
     * 7 now that we have a sub graph, change some verteces into varaibles as well as some edges
     * and we have a query graph ready
     */




    private int numberOfNodes;
    private int maxNumberofEdges;
    private int numberOfVaribles;
    private HashMap<Integer , VertexGraph> graph;
    private HashMap<Integer,ArrayList<Triple> > tripleGraph;
    private ArrayList<TriplePattern> queryAnswer;
    private ArrayList<TriplePattern> triplePatterns;
    private ArrayList<Query> queries;
    private int maxQueryFrquency;


    public QueryGenrator(int numberOfNodes, int maxNumberofEdges, HashMap<Integer , VertexGraph> graph , int numberOfVaribles, HashMap<Integer,ArrayList<Triple> > tripleGraph , int maxQueryFrquency){
        this.numberOfNodes = numberOfNodes;
        this.maxNumberofEdges = maxNumberofEdges;
        this.graph = graph;
        this.numberOfVaribles = numberOfVaribles;
        this.tripleGraph = tripleGraph;
        this.maxQueryFrquency = maxQueryFrquency;
        queries = new ArrayList();
    }

/*
    public ArrayList<QueryStuff.Query> buildQueries(int numberOfQueries) {
        for(int i= 0 ; i<numberOfQueries ; i++){
            triplePatterns = new ArrayList();
            buildQuery();
            QueryStuff.Query query = new QueryStuff.Query(triplePatterns , new Random().nextInt(maxQueryFrquency) +1 ,queryAnswer );
            queries.add(query);
        }
        return queries;
    }*/

    public ArrayList<Query> buildHeavyQueries(int numberOfQueries) {
        for(int i= 0 ; i<numberOfQueries ; i++){
            triplePatterns = new ArrayList();
            if(buildHeavyQuery() != null) {
                Query query = new Query(triplePatterns, new Random().nextInt(maxQueryFrquency) + 1, queryAnswer);
                queries.add(query);
            }else
                i--;
        }
        return queries;
    }


    int countIgnore = 0;
    private ArrayList<TriplePattern> buildHeavyQuery(){
        int processedNode = 0;
        Queue<Integer> toProcess = new ArrayDeque();
        ArrayList<Integer> resultedVertices = new ArrayList();
        int s = graph.size();
        int stratingNode;
        VertexGraph startingVertexGraph;
        int trialCount =0;
        do {
            stratingNode = new Random().nextInt(s) + 1;
            startingVertexGraph = graph.get(stratingNode);
            if(trialCount > 100) {
                System.err.println(" problem detected: not able to find starting node");
                System.exit(2);
            }
        }while (startingVertexGraph == null ||  graph.get(stratingNode).edgesVertex.size() == 0);
        toProcess.add(stratingNode);
        HashMap<Integer, Integer> vistedVertices = new HashMap();
        while(toProcess.size() > 0 && resultedVertices.size() < numberOfNodes) {
            Integer toProcessVertex = toProcess.remove();
            vistedVertices.put(toProcessVertex,toProcessVertex);
            ArrayList<Integer> v = graph.get(toProcessVertex).edgesVertex;
            resultedVertices.add(toProcessVertex);
            Random ran = new Random();
            int index ;
            trialCount =0;
            boolean circularDetected = false;
            do {
                index = ran.nextInt(v.size());
                trialCount++;
                if(trialCount > 100) {
                    if(resultedVertices.size() < 3) {
                        if(countIgnore == 0)
                            MainUinAdapt.printBuffer(0,countIgnore+":ignoring samll query .."+resultedVertices.size());
                        else
                            MainUinAdapt.printBuffer(1,countIgnore+":ignoring samll query .."+resultedVertices.size());
                        countIgnore++;
                        //System.out.println("ignoring samll query ..");
                        return null;
                    }
                    System.err.println(" problem detected in circular query generation , we took only :"+resultedVertices.size()+" nodes");
                    //System.exit(2);
                    circularDetected = true;
                    break;
                }

            } while (vistedVertices.containsKey(v.get(index)) || graph.get(v.get(index)).edgesVertex.size() == 0);
            System.err.println("getting edge index :"+index +"/"+v.size());
            if(circularDetected)
                break;
            toProcess.add(v.get(index));
        }
        processQueryGraph(resultedVertices , true);
        System.out.println("done building query");
        return triplePatterns;

    }

    public static ArrayList<String> buildFastHeavyQuery(MyHashMap<String, ArrayList<Triple>> OPxP, HashMap<Integer, ArrayList<Triple>> OPS, int max_id, Dictionary reverseDicitionary, ArrayList<String> queryKeys, double memPercent){
        if(queryKeys == null)
            return null ;
        if(queryKeys.size() == 0)
            queryKeys = getHeavyQuerykeys(OPxP , memPercent);
        ArrayList<ArrayList<TriplePattern>> quereis = new ArrayList();

        for(int j = 0 ; j<queryKeys.size() ; j++){
            ArrayList<Triple> list =  OPxP.get( queryKeys.get(j));
            ArrayList<TriplePattern> locTriplePatterns = new ArrayList();
            for(int i=0 ; i < list.size() ; i+=2){
                Triple triple1 = list.get(i);
                Triple Triple = list.get(i+1);
                int s = Triple.triples[0];
                ArrayList<Triple> list2 = OPS.get(s);
                if(list2!=null){
                    Triple triple3 = list2.get(0);
                    TriplePattern triplePattern3 = new TriplePattern(-3,triple3.triples[1],-2);
                    TriplePattern triplePattern2 = new TriplePattern(-2,Triple.triples[1],-1);
                    TriplePattern triplePattern1 = new TriplePattern(-1,triple1.triples[1],triple1.triples[2]);
                    String tkey = triple1.triples[2]+""+triple1.triples[1]+""+Triple.triples[1];
                    ArrayList<Triple> listtt = OPxP.get(tkey);
                    triplePattern1.tempID = tkey;triplePattern2.tempID = tkey;triplePattern3.tempID = tkey;
                    locTriplePatterns.add(triplePattern1);
                    locTriplePatterns.add(triplePattern2);
                    locTriplePatterns.add(triplePattern3);
                    quereis.add(locTriplePatterns);
                    break;
                }
            }
        }
        /*
        Iterator it = OPxP.entrySet().iterator();
        while(it.hasNext()){
            ArrayList<TriplePattern> locTriplePatterns = new ArrayList();
            Map.Entry pair = (Map.Entry)it.next();
           // Integer O = (Integer) pair.getKey();
           // ArrayList<Triple> list = (ArrayList<Triple>) pair.getValue();
            ArrayList<Triple> list =  OPxP.getArrayList((String) pair.getValue());
            for(int i=0 ; i < list.size() ; i+=2){
                Triple triple1 = list.get(i);
                Triple Triple = list.get(i+1);
                int s = Triple.triples[0];
                ArrayList<Triple> list2 = OPS.get(s);
                if(list2!=null){
                    Triple triple3 = list2.get(0);
                    TriplePattern triplePattern3 = new TriplePattern(-3,triple3.triples[1],-2);
                    TriplePattern triplePattern2 = new TriplePattern(-2,Triple.triples[1],-1);
                    TriplePattern triplePattern1 = new TriplePattern(-1,triple1.triples[1],triple1.triples[2]);
                    String tkey = triple1.triples[2]+""+triple1.triples[1]+""+Triple.triples[1];
                    ArrayList<Triple> listtt = OPxP.get(tkey);
                    triplePattern1.tempID = tkey;triplePattern2.tempID = tkey;triplePattern3.tempID = tkey;
                    locTriplePatterns.add(triplePattern1);
                    locTriplePatterns.add(triplePattern2);
                    locTriplePatterns.add(triplePattern3);
                    quereis.add(locTriplePatterns);
                    break;
                }
            }
        }*/
       ArrayList<String> quereisStrList = new ArrayList();
        for(int jj = 0 ; jj<quereis.size() ; jj++) {
         //   int ch = new Random().nextInt(quereis.size()) + 1;
            ArrayList<TriplePattern> triplePatterns = quereis.get(jj);
            String vars = "?x1 ?x2 ?x3 ?x4 ";
            String predicates = "?x1 " + reverseDicitionary.get(triplePatterns.get(2).triples[1]) + " ?x2.";
            predicates += "?x2 " + reverseDicitionary.get(triplePatterns.get(1).triples[1]) + " ?x3.";
            predicates += "?x3 " + reverseDicitionary.get(triplePatterns.get(0).triples[1]) + " " + reverseDicitionary.get(triplePatterns.get(0).triples[2]);
            String SPARQL = "select " + vars + " where {" + predicates + "}";
            quereisStrList.add(triplePatterns.get(0).tempID+" : "+SPARQL);
        }
        return quereisStrList;


    }

    private static ArrayList<String> getHeavyQuerykeys(MyHashMap<String, ArrayList<Triple>> OPxP, double memPercent) {
      /*  int max_key = 1000000;
        boolean up = false;
        if(dictionary.containsKey(max_key))
            up = true;
        while (true){
            if(up) {
                if (dictionary.containsKey(max_key))
                    max_key++;
                else
                    break;
            }else {
                if (!dictionary.containsKey(max_key))
                    max_key--;
                else
                    break;
            }
        }*/
        ArrayList<String> keys = new ArrayList();
        ArrayList<String> backupKeys = new ArrayList();
        int cnt = 0;
        Random random = new Random();
        //Iterator it = OPxP.entrySet().iterator();
        boolean memItActive = false;
        Iterator it = OPxP.getQueryTimeIterator();
        if(it == null || memPercent == 0) {
            it = OPxP.entrySet().iterator();
            memItActive = false;
        }
        while(keys.size() < 50 && it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            int choice = random.nextInt(10000);
            if(choice == 100) {
                String key = (String) pair.getKey();
                keys.add(key);
                if(memItActive && keys.size() >= memPercent*50) {
                    it = OPxP.entrySet().iterator();
                    memItActive = false;
                }
            }
            if(choice%800 == 0){
                String key = (String) pair.getKey();
                backupKeys.add(key);
            }
            cnt++;
        }

        int i=0;
        while (keys.size() < 50){
            keys.add(backupKeys.get(i));
            i++;
        }
        return keys;
    }

    /*
        private ArrayList<triple.TriplePattern> buildQuery(){
            int processedNode = 0;
            Queue<Integer> toProcess = new ArrayDeque();
            ArrayList<Integer> resultedVertices = new ArrayList();
           int s = graph.size();
           int stratingNode = new Random().nextInt(s) + 1;
            toProcess.add(stratingNode);
            while(toProcess.size() > 0 && resultedVertices.size() < numberOfNodes) {
                Integer toProcessVertex = toProcess.remove();
                ArrayList<triple.Vertex> v = graph.get(toProcessVertex);
                int currentMaxEdges = new Random().nextInt(maxNumberofEdges) + 1;
                resultedVertices.add(toProcessVertex);
                Random ran = new Random();
                HashMap<Integer, Integer> unique = new HashMap();
                for (int i = 0; i < v.size() && i < currentMaxEdges; i++) {
                    int index = 0;
                    if (v.size() - 3 > currentMaxEdges) {
                        do {
                            index = ran.nextInt(v.size());
                        } while (unique.containsKey(index));

                        unique.put(index, index);
                    } else
                        index = i;
                    toProcess.add(v.get(index).v);
                }
            }
            processQueryGraph(resultedVertices , false);
            return triplePatterns;

        }
    */
    private ArrayList<TriplePattern> processQueryGraph(ArrayList<Integer> resultedVertices , boolean heavy){
        for(int i= 0 ; i< resultedVertices.size() ; i++){
            int l = resultedVertices.get(i);
            ArrayList<Triple> triples = tripleGraph.get(l);
            if(triples == null)
                continue;

            for(int j=0 ; j<triples.size() ; j++){
                int matchCount = 0;
                for(int k= 0 ; k<resultedVertices.size() ; k++) {
                    int r = resultedVertices.get(k);
                    int t = triples.get(j).triples[2];
                    if (r == t) {
                        triplePatterns.add(new TriplePattern(triples.get(j).triples[0], triples.get(j).triples[1], triples.get(j).triples[2]));
                        matchCount++;
                    }
                }
               // System.out.println("match count is: "+matchCount);
            }
        }
        queryAnswer = new ArrayList();
        for(int s=0;s<triplePatterns.size() ; s++){
            queryAnswer.add(new TriplePattern(triplePatterns.get(s).triples[0] , triplePatterns.get(s).triples[1] , triplePatterns.get(s).triples[2]));
        }
        assignVariablesToTriples(triplePatterns, heavy);
        return triplePatterns;

    }

    private void assignVariablesToTriples(ArrayList<TriplePattern> triplePatterns, boolean heavy) {
        //always leave the propertyies !
        //for each tripel you have three choices: s is var , o is var , or s&o are vars
        //randomly select one of the choices until the number of vars is satisfied
        int assigneVra = 0;
        HashMap<Integer , Integer> variablesIndex  = new HashMap<Integer , Integer>();
        for(int i=0 ; i<triplePatterns.size() ; i++){
            int varS = 0 , varO = 0;
            triplePatterns.get(i).variablesIndex = variablesIndex;
            if(!heavy) {
                if (variablesIndex.containsKey(triplePatterns.get(i).triples[0])) {
                    varS = variablesIndex.get(triplePatterns.get(i).fixedTriples[0]);
                    triplePatterns.get(i).triples[0] = TriplePattern.thisIsVariable;
                }
                if (variablesIndex.containsKey(triplePatterns.get(i).triples[2])) {
                    varO = variablesIndex.get(triplePatterns.get(i).fixedTriples[2]);
                    triplePatterns.get(i).triples[2] = TriplePattern.thisIsVariable;
                }
                if (varS != 0 || varO != 0)
                    continue;
            }
            int bounds = 2;
            if(assigneVra <= numberOfVaribles)
                bounds = 3;
            int choice;
            if(!heavy)
                choice = new Random().nextInt(3);
            else{
                // in heavy query every triple pattern has two variables in s and o , except the last triple pattern which has two one variable at s.
                choice = 2;
                if( i == triplePatterns.size()-1)
                    choice = 0;
            }
            switch (choice){
                case 0 :
                    assigneVra++;
                    triplePatterns.get(i).triples[0] = TriplePattern.thisIsVariable;
                    variablesIndex.put(triplePatterns.get(i).fixedTriples[0] ,-1 * assigneVra);
                    break;
                case 1:
                    assigneVra++;
                    triplePatterns.get(i).triples[2] = TriplePattern.thisIsVariable;
                    variablesIndex.put(triplePatterns.get(i).fixedTriples[2] ,-1 * assigneVra);
                    break;
                case 2:
                    assigneVra++;
                    triplePatterns.get(i).triples[0] = TriplePattern.thisIsVariable;
                    variablesIndex.put(triplePatterns.get(i).fixedTriples[0] ,-1 * assigneVra);
                    assigneVra++;
                    triplePatterns.get(i).triples[2] = TriplePattern.thisIsVariable;
                    variablesIndex.put(triplePatterns.get(i).fixedTriples[2] ,-1 * assigneVra);
                    break;
            }
        }
    }

}
