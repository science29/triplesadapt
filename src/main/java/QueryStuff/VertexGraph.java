package QueryStuff;

import java.util.ArrayList;
import java.util.HashMap;

public class VertexGraph {

   public int ID ;
    public ArrayList<Integer> edgesVertex;
    private ArrayList<Integer> edgesPredicate;

    public int partitionNumber ;
    public HashMap<Integer,Integer> linkedInPartitions;
    public int dist;

    public HashMap<Integer,Integer> partutPartitionsMap;

    public boolean writtenToFile =false; //to indicate that this vertex is already been written sometime ago to file or not

    public HashMap<Integer,Integer> writenToPartitionsFilesMap ; //key is the partition while value is the tiple positioin ie 0 = s , 1 = p , 2=o

    public VertexGraph(int id){
        ID = id;
        edgesVertex = new ArrayList();
        edgesPredicate = new ArrayList();
        dist = -1;
        partitionNumber = -1;
        linkedInPartitions = new HashMap();
        writenToPartitionsFilesMap = new  HashMap();
    }
    public boolean addLinkInPartition(int partitionNumber){
        Integer p = new Integer(partitionNumber);
        if(!linkedInPartitions.containsKey(p)) {
            linkedInPartitions.put(p, p);
            return true;
        }
        return false;
    }

    public ArrayList<Integer> getLinkedPartitions() {
        ArrayList<Integer> res =new ArrayList<Integer>(linkedInPartitions.values());
        return res;
    }

    public void addEdge(int toVertexID , int prediacte){

        edgesPredicate.add(prediacte);
        edgesVertex.add(toVertexID);

    }


}
