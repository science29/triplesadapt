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

    HashMap<String , Query> f;

    Graph queryGraph;

    HashMap<Long , Integer> heatMap;

    public HeatQuery(Query query){
        HashMap<TriplePattern, ArrayList<Triple>> answerMap = query.getAnswerMap();

        queryGraph = new Graph();

        Iterator it = answerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            ArrayList<Triple> list = (ArrayList<Triple>) pair.getValue();
            for(int i = 0 ; i < list.size() ; i++){
                Triple triple = list.get(i);
                triple.triples[0]
            }
        }
    }

    public boolean connectQuery(Query query){

    }


    public boolean combineHeatQuery(HeatQuery another){

    }

    public int getHeat(Long vertexID){}



    private class Graph{
        ArrayList<Long> V;
        HashMap<Long , ArrayList<Edge>> E;

        public Graph(){
            V = new ArrayList<Long>();
        }
        public void add(Long V , Long p , Long e){

        }
    }
    private class Edge{
        Long e;
        Long p;
    }



}
