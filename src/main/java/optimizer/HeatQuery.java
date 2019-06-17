package optimizer;

import QueryStuff.Query;
import QueryStuff.VertexGraph;
import org.mapdb.Atomic;
import triple.Triple;
import triple.TriplePattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class HeatQuery {


    Graph queryGraph;

    HashMap<Long , Integer> vertexHeatMap;
    HashMap<Triple , Integer> tripleHeatMap;
    HashMap<Triple , Integer> annonmizedTripleHeatMap;

    public HeatQuery(Query query){
        queryGraph = new Graph();
        vertexHeatMap = new HashMap<Long, Integer>();
        tripleHeatMap = new HashMap<Triple, Integer>();
        annonmizedTripleHeatMap = new HashMap<Triple, Integer>();
        addQuery(query);
    }

    public boolean connectQuery(Query query){
        HashMap<TriplePattern, ArrayList<Triple>> answerMap = query.getAnswerMap();
        boolean connected = false;
        Iterator it = answerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            ArrayList<Triple> list = (ArrayList<Triple>) pair.getValue();
            for(int i = 0 ; i < list.size() ; i++){
                Triple triple = list.get(i);
                if(queryGraph.isExist(triple)) {
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
            ArrayList<Triple> list = (ArrayList<Triple>) pair.getValue();
            for(int i = 0 ; i < list.size() ; i++){
                Triple triple = list.get(i);
                queryGraph.add(triple);
                adjustHeat(triple);
            }
        }
    }

    private void adjustHeat(Triple triple){
       adjustVertexHeat(triple.triples[0]);
       adjustVertexHeat(triple.triples[1]);
       adjustTripleHeat(triple);
    }


    private void adjustTripleHeat(Triple triple){
        Integer heat = tripleHeatMap.get(triple);
        if(heat != null){
            heat++;
        }else{
            tripleHeatMap.put(triple , 1);
        }
    }

    private void adjustVertexHeat(Long vertex){
        Integer heat = vertexHeatMap.get(vertex);
        if(heat != null){
            heat++;
        }else{
            vertexHeatMap.put(vertex , 1);
        }
    }



    public int getHeat(Long vertexID){
        if(vertexHeatMap.containsKey(vertexID))
            return vertexHeatMap.get(vertexID);
        return 0;
    }



    private class Graph{
        ArrayList<Long> V;
        HashMap<Long , ArrayList<Triple>> E;
        HashMap<String , Long > edgesMap;


        public Graph(){
            V = new ArrayList<Long>();
        }

        public void add(Long s, Long p , Long o){
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


        public boolean isExist(Triple triple){
            if(E.containsKey(triple.triples[0])) {
               // add(triple.triples[0] , triple.triples[1] , triple.triples[2]);
                return true;
            }

            if(E.containsKey(triple.triples[2])) {
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
        Long o;
        Long p;
        public Edge(long o , long p){
            this.e = e;
            this.p = p;
        }
    */



}
