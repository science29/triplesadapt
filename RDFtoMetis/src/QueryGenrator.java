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
    private HashMap<Long , VertexGraph> graph;
    private HashMap<Long,ArrayList<Triple> > tripleGraph;
    private ArrayList<TriplePattern> queryAnswer;
    private ArrayList<TriplePattern> triplePatterns;
    private ArrayList<Query> queries;
    private int maxQueryFrquency;


    public QueryGenrator(int numberOfNodes, int maxNumberofEdges, HashMap<Long , VertexGraph> graph , int numberOfVaribles, HashMap<Long,ArrayList<Triple> > tripleGraph , int maxQueryFrquency){
        this.numberOfNodes = numberOfNodes;
        this.maxNumberofEdges = maxNumberofEdges;
        this.graph = graph;
        this.numberOfVaribles = numberOfVaribles;
        this.tripleGraph = tripleGraph;
        this.maxQueryFrquency = maxQueryFrquency;
        queries = new ArrayList<>();
    }

/*
    public ArrayList<QueryStuff.Query> buildQueries(int numberOfQueries) {
        for(int i= 0 ; i<numberOfQueries ; i++){
            triplePatterns = new ArrayList<>();
            buildQuery();
            QueryStuff.Query query = new QueryStuff.Query(triplePatterns , new Random().nextInt(maxQueryFrquency) +1 ,queryAnswer );
            queries.add(query);
        }
        return queries;
    }*/

    public ArrayList<Query> buildHeavyQueries(int numberOfQueries) {
        for(int i= 0 ; i<numberOfQueries ; i++){
            triplePatterns = new ArrayList<>();
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
        Queue<Long> toProcess = new ArrayDeque<>();
        ArrayList<Long> resultedVertices = new ArrayList<>();
        int s = graph.size();
        long stratingNode;
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
        HashMap<Long, Long> vistedVertices = new HashMap<>();
        while(toProcess.size() > 0 && resultedVertices.size() < numberOfNodes) {
            Long toProcessVertex = toProcess.remove();
            vistedVertices.put(toProcessVertex,toProcessVertex);
            ArrayList<Long> v = graph.get(toProcessVertex).edgesVertex;
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
                            Main2.printBuffer(0,countIgnore+":ignoring samll query .."+resultedVertices.size());
                        else
                            Main2.printBuffer(1,countIgnore+":ignoring samll query .."+resultedVertices.size());
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

/*
    private ArrayList<triple.TriplePattern> buildQuery(){
        int processedNode = 0;
        Queue<Long> toProcess = new ArrayDeque<>();
        ArrayList<Long> resultedVertices = new ArrayList<>();
       int s = graph.size();
       long stratingNode = new Random().nextInt(s) + 1;
        toProcess.add(stratingNode);
        while(toProcess.size() > 0 && resultedVertices.size() < numberOfNodes) {
            Long toProcessVertex = toProcess.remove();
            ArrayList<triple.Vertex> v = graph.get(toProcessVertex);
            int currentMaxEdges = new Random().nextInt(maxNumberofEdges) + 1;
            resultedVertices.add(toProcessVertex);
            Random ran = new Random();
            HashMap<Integer, Integer> unique = new HashMap<>();
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
    private ArrayList<TriplePattern> processQueryGraph(ArrayList<Long> resultedVertices , boolean heavy){
        for(int i= 0 ; i< resultedVertices.size() ; i++){
            long l = resultedVertices.get(i);
            ArrayList<Triple> triples = tripleGraph.get(l);
            if(triples == null)
                continue;

            for(int j=0 ; j<triples.size() ; j++){
                int matchCount = 0;
                for(int k= 0 ; k<resultedVertices.size() ; k++) {
                    long r = resultedVertices.get(k);
                    long t = triples.get(j).triples[2];
                    if (r == t) {
                        triplePatterns.add(new TriplePattern(triples.get(j).triples[0], triples.get(j).triples[1], triples.get(j).triples[2]));
                        matchCount++;
                    }
                }
               // System.out.println("match count is: "+matchCount);
            }
        }
        queryAnswer = new ArrayList<>();
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
        HashMap<Long , Integer> variablesIndex  = new HashMap<Long , Integer>();
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
