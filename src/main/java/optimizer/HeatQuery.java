package optimizer;

import QueryStuff.Query;
import QueryStuff.VertexGraph;
import org.mapdb.Atomic;
import triple.Triple;
import triple.TriplePattern;
import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class HeatQuery {


    Graph queryGraph;

    HashMap<Integer , Integer> vertexHeatMap;
    HashMap<TriplePattern2, Integer> tripleHeatMap;
    HashMap<TriplePattern2 , Integer> annonmizedTripleHeatMap;

    public HeatQuery(Query query){
        queryGraph = new Graph();
        vertexHeatMap = new HashMap<Integer, Integer>();
        tripleHeatMap = new HashMap<TriplePattern2, Integer>();
        annonmizedTripleHeatMap = new HashMap<TriplePattern2, Integer>();
        addQuery(query);
    }

    public boolean connectQuery(Query query){
        HashMap<TriplePattern, ArrayList<Triple>> answerMap = query.getAnswerMap();
        boolean connected = false;
        Iterator it = answerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            ArrayList<TriplePattern2> list = (ArrayList<TriplePattern2>) pair.getValue();
            for(int i = 0 ; i < list.size() ; i++){
                TriplePattern2 triplePattern = list.get(i);
                if(queryGraph.isExist(triplePattern.getTriples())) {
                    addQuery(query);
                    return true;
                }
            }

        }
       return false;
    }

    private void addQuery(Query query){
        HashMap<TriplePattern, ArrayList<Triple>> answerMap = query.getAnswerMap();
        Iterator it = answerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            ArrayList<TriplePattern2> list = (ArrayList<TriplePattern2>) pair.getValue();
            for(int i = 0 ; i < list.size() ; i++){
                TriplePattern2 triplePattern = list.get(i);
              //  queryGraph.add();
               // adjustHeat(triple);
            }
        }
    }

    private void adjustHeat(TriplePattern2 triplePattern){
       adjustVertexHeat(triplePattern.getTriples()[0]);
       adjustVertexHeat(triplePattern.getTriples()[1]);
       adjustTripleHeat(triplePattern);
    }

    private void anonymize(){
        
    }


    private void adjustTripleHeat(TriplePattern2 triplePattern){
        Integer heat = tripleHeatMap.get(triplePattern);
        if(heat != null){
            heat++;
        }else{
            tripleHeatMap.put(triplePattern , 1);
        }
    }

    private void adjustVertexHeat(Integer vertex){
        Integer heat = vertexHeatMap.get(vertex);
        if(heat != null){
            heat++;
        }else{
            vertexHeatMap.put(vertex , 1);
        }
    }



    public int getFullVertexHeat(Integer vertexID){
        if(vertexHeatMap.containsKey(vertexID))
            return vertexHeatMap.get(vertexID);
        return 0;
    }



    private class Graph{
        ArrayList<Integer> V;
        HashMap<Integer , ArrayList<Triple>> E;
        HashMap<String , Integer > edgesMap;


        public Graph(){
            V = new ArrayList<Integer>();
        }

        public void add(Integer s, Integer p , Integer o){
            String key = s+","+o;
            if(!edgesMap.containsKey(key)){
                edgesMap.put(key , p);
                ArrayList<Triple> edgeList = E.get(s);
                if(edgeList == null) {
                    edgeList = new ArrayList<Triple>();
                    V.add(s);
                }
                edgeList.add(new Triple(s,p,o));
            }

        }


        public boolean isExist(int [] triple){
            if(E.containsKey(triple[0])) {
               // add(triple.triples[0] , triple.triples[1] , triple.triples[2]);
                return true;
            }

            if(E.containsKey(triple[2])) {
               // add(triple.triples[0] , triple.triples[1] , triple.triples[2]);
                return true;
            }
            return false;
        }

        public void add(Triple triple) {
            String key = triple.triples[0]+","+triple.triples[2];
            if(!edgesMap.containsKey(key)){
                edgesMap.put(key , triple.triples[1]);
                ArrayList<Triple> edgeList = E.get(triple.triples[0]);
                if(edgeList == null) {
                    edgeList = new ArrayList<Triple>();
                    V.add(triple.triples[0]);
                }
                edgeList.add(triple);
            }
        }
    }
    /*private class Edge{
        Integer o;
        Integer p;
        public Edge(int o , int p){
            this.e = e;
            this.p = p;
        }
    */



}
