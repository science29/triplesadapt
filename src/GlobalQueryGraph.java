import triple.Triple;
import triple.TriplePattern;

import java.util.ArrayList;
import java.util.HashMap;


/**
 * we need to have a ranking function to each minterm, so that the we can use the best fragment while
 * we  still keep the connectivity between fragments.
 *
 * The data to be partitioned is the RDF data that lies at the boundary of the METIS partitions .
 * We consider in building the query graph all the quereis .
 * enhancing the load balance by playing with depth to apply the replication
 * idea> build the noraml fragment graph then make use of it according to the needs.
 * ...
 * since we will consider only the part of the fragments which lies on the edges, we can have higher normallizationThreshold, which create bigger fragments.
 */
public class GlobalQueryGraph {

    private final int normallizationThreshold;
    private final ArrayList<Query> annomizedQueries;
    ArrayList<Query>  queries;
    ArrayList<AnnomizedTriple> annomizedTriples;

    HashMap<Long,ArrayList<Triple> > POS;
    HashMap<Long,ArrayList<Triple> > SPO;
    HashMap<Long,ArrayList<Triple> > OPS;
    HashMap<Long, VertexGraph> vertexIndex;

   public GlobalQueryGraph(ArrayList<Query> queries, int normallizationThreshold, HashMap<Long, ArrayList<Triple>> pos, HashMap<Long, ArrayList<Triple>> spo, HashMap<Long, ArrayList<Triple>> ops, HashMap<Long, VertexGraph> VertexIndex){
       this.queries = queries;
       this.annomizedQueries = new ArrayList<>();
       //copying the queries into annomized query
       for(int i=0 ; i<queries.size();i++){
           ArrayList<TriplePattern> TriplePatterns_new = new ArrayList<>();
           for(int j = 0 ; j < queries.get(i).triplePatterns.size() ; j++)
               TriplePatterns_new.add(new TriplePattern(queries.get(i).triplePatterns.get(j).triples[0]  , queries.get(i).triplePatterns.get(j).triples[1]  ,queries.get(i).triplePatterns.get(j).triples[2]));
           annomizedQueries.add(new Query(TriplePatterns_new, queries.get(i).queryFrquency , queries.get(i).simpleAnswer )) ;
       }
       this.normallizationThreshold = normallizationThreshold;
       annomizedTriples = new ArrayList<>();
       this.POS = pos;
       this.SPO = spo;
       this.OPS = ops;
       this.vertexIndex = VertexIndex;
   }

   private void annomize(){
       //do the annmization
       HashMap<Long , Integer> constants = new HashMap<>();
       for(int i= 0; i<queries.size() ; i++)
           for(int j = 0; j<queries.get(i).triplePatterns.size() ; j++)
               for(int k =0 ; k< queries.get(i).triplePatterns.get(j).triples.length ; k++) {
                    //skip the property in the triple patteren
                   if(k == 1)
                       continue;
                   if (queries.get(i).triplePatterns.get(j).triples[k] != TriplePattern.thisIsVariable) {
                       long key = queries.get(i).triplePatterns.get(j).triples[k];
                       if (constants.containsKey(key)) {
                           constants.replace(key, constants.get(key) + queries.get(i).queryFrquency);
                       } else
                           constants.put(key, queries.get(i).queryFrquency);
                       if (constants.get(key) <= normallizationThreshold)
                           annomizedQueries.get(i).triplePatterns.get(j).triples[k] = TriplePattern.thisIsVariable;
                   }
               }
   }


//build the connection between the annomized triples and generate the global query graph
   private void buildAnnomizedTripleList(){
       HashMap<Integer , Integer> constants = new HashMap<>();
       for(int i= 0; i<annomizedQueries.size() ; i++)
           for(int j = 0; j<annomizedQueries.get(i).triplePatterns.size() ; j++){
               AnnomizedTriple annomRef = getAnnomizedTripelRef(annomizedQueries.get(i).triplePatterns.get(j));
               if(annomRef == null){
                   annomRef = new AnnomizedTriple(annomizedQueries.get(i).triplePatterns.get(j));
                   annomizedTriples.add(annomRef);
                   annomRef.createFragment(POS , OPS ,SPO , vertexIndex);
               }
               annomRef.frequency =  annomRef.frequency + annomizedQueries.get(i).queryFrquency;
               for (int k = 0; k < annomizedQueries.get(i).triplePatterns.size() ; k++ ) {
                   AnnomizedTriple annomRef2 = getAnnomizedTripelRef(annomizedQueries.get(i).triplePatterns.get(j));
                   if(annomRef2 == null){
                       annomRef2 = new AnnomizedTriple(annomizedQueries.get(i).triplePatterns.get(j));
                       annomizedTriples.add(annomRef2);
                       annomRef2.createFragment(POS , OPS ,SPO, vertexIndex);
                   }
                   annomRef.addConnection(annomRef2 , annomizedQueries.get(i).queryFrquency );
               }
           }
   }




    private AnnomizedTriple getAnnomizedTripelRef(TriplePattern annomizedTrip){
        for(int i= 0 ; i < annomizedTriples.size() ; i++){
            String tripleString1 = annomizedTrip.triples[0] + "-" + annomizedTrip.triples[1] +"-"+annomizedTrip.triples[2] ;
            String tripleString2 = annomizedTriples.get(i).triplePattern.triples[0] + "-" + annomizedTriples.get(i).triplePattern.triples[1] +"-"+annomizedTriples.get(i).triplePattern.triples[2] ;
            if (tripleString1.matches(tripleString2))
                return annomizedTriples.get(i);
        }
        return null;
    }
   public void genrate(){
      annomize();
      buildAnnomizedTripleList();
       //do the
   }

   public int getEdgeFrequency(Triple triple){
       int matchedEdgeCount = 0;
       for(int i= 0; i<queries.size() ; i++)
           for(int j = 0; j<queries.get(i).triplePatterns.size() ; j++) {
               int match = 0;
               for (int k = 0; k < queries.get(i).triplePatterns.get(j).triples.length; k++) {
                   if (queries.get(i).triplePatterns.get(j).triples[k] != TriplePattern.thisIsVariable) {
                       long keyQuery = queries.get(i).triplePatterns.get(j).triples[k];
                       long keyTriple = triple.triples[k];
                       if (keyQuery != keyTriple)
                           break;
                   }
                   match++;
               }
               if(match == 3)
                   matchedEdgeCount+= queries.get(i).queryFrquency;
           }
           return matchedEdgeCount;
   }

    public ArrayList<AnnomizedTriple> getAnnomizedTriples() {
        return annomizedTriples;
    }

    public static void setDynamicWeights(ArrayList<Query> queries, HashMap<Long, ArrayList<Triple>> tripleGraph, HashMap<Long, ArrayList<Vertex>> graph, HashMap<Long, ArrayList<Triple>> POS) {
       for(int i=0 ; i<queries.size() ; i++){
          Query query = queries.get(i);
          try {
              for (int j = 0; j < query.simpleAnswer.size(); j++) {
                  TriplePattern answer = query.simpleAnswer.get(j);
                  long s = answer.triples[0];
                  long o = answer.triples[2];
                  ArrayList<Vertex> graph_O_ListTothis_S = graph.get(s);
                  if(graph_O_ListTothis_S == null)
                      continue;
                  for (int k = 0; k < graph_O_ListTothis_S.size(); k++) {
                      if (graph_O_ListTothis_S.get(k).v == o && graph_O_ListTothis_S.get(k).e != -1)
                          graph_O_ListTothis_S.get(k).weight1 += query.queryFrquency;
                  }


                  ArrayList<Triple> propertyTripleList = POS.get(answer.triples[1]);
                  for (int k = 0; k < propertyTripleList.size(); k++) {
                      Triple triple = propertyTripleList.get(k);
                      ArrayList<Vertex> list = graph.get(triple.triples[0]);
                      for (int m = 0; m < list.size(); m++) {
                          if (list.get(m).v == triple.triples[2] && list.get(m).e != -1)
                              list.get(m).weight2 += query.queryFrquency; //important note : the weight2 field here is related to the edge between vertex m and list.get(m).v
                      }
                  }
              }

          }
          catch (NullPointerException e){
              e.printStackTrace();
          }

       }
    }


    /**
     * assign each fragment to the most benifticial partition using prartut assiging method
     * @param numberOfpartitions
     */
    public void assignPartut(int numberOfpartitions){
       double [] benefit = new double[numberOfpartitions];

       //for each fragment we calculate the benefit function of each partition
        double fixedPartionShare = getFixedLoadPerPartition(numberOfpartitions);
        for(int i= 0; i<annomizedTriples.size() ; i++){

            AnnomizedTriple annomizedTriple = annomizedTriples.get(i);
            double maxBeifit = 0; int maxBenifitPartion = 0;
            for(int j =0 ; j < numberOfpartitions ; j++){
                double partitionLoad = getPartutLoad(j);
                double connectWeight = getConnectWeight(i,j);
                double fragmentLoad = getFragmentLoad(i);
                benefit[j] = (2*fixedPartionShare/(fixedPartionShare + partitionLoad) ) + connectWeight;
                if(benefit[j] > maxBeifit){
                    maxBeifit = benefit[j];
                    maxBenifitPartion  = j;
                }
            }
            annomizedTriple.partutAssignedPartition = maxBenifitPartion ;
            if(annomizedTriple.partutAssignedPartition  == -1)
                annomizedTriple.partutAssignedPartition = -1;
        }
    }


    private double getPartutLoad(int partitionNumber) {
        int load = 0;
        for(int i = 0 ; i < annomizedTriples.size() ; i++){
            if(annomizedTriples.get(i).partutAssignedPartition == partitionNumber){
                load = load + annomizedTriples.get(i).frequency *  annomizedTriples.get(i).fragment.getSize();
            }
        }
        return load;
    }

    private double getFixedLoadPerPartition(int numberOfPartition){
        int load = 0;
        for(int i = 0 ; i < annomizedTriples.size() ; i++){
            load = load + annomizedTriples.get(i).frequency *  annomizedTriples.get(i).fragment.getSize();
        }
        double partioinShare = ((double)load)/((double)numberOfPartition);
        return partioinShare;
    }

    private double getFragmentLoad(int fragmentIndex) {
       return (annomizedTriples.get(fragmentIndex).frequency *  annomizedTriples.get(fragmentIndex).fragment.getSize());
    }

    private double getConnectWeight(int  fragmentIndex , int partitionNumber) {
        double resWeight = 0 ;
        AnnomizedTriple reqFragment = annomizedTriples.get(fragmentIndex);
        for (int i = 0; i < annomizedTriples.size(); i++) {
            if (annomizedTriples.get(i).partutAssignedPartition == partitionNumber) {
                ArrayList<AnnomizedTriple> connAnnomizedTriples = annomizedTriples.get(i).connectedTriples;
                for (int j = 0; j < connAnnomizedTriples.size(); j++){
                    if (connAnnomizedTriples.get(j) == reqFragment) {
                        resWeight = resWeight + annomizedTriples.get(i).connectetionFrequency.get(j);
                    }
                }

            }
        }
        return resWeight;
    }
}
