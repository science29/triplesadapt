import java.util.ArrayList;
import java.util.HashMap;

public class AnnomizedTriple{
    public TriplePattern triplePattern;
    public int frequency;
    public ArrayList<AnnomizedTriple> connectedTriples;
    public HashMap<AnnomizedTriple , Integer> connectetionFrequency;

    public Fragment fragment;
    public int partutAssignedPartition;

    public  AnnomizedTriple(TriplePattern triple){
        this.triplePattern = triple ;
        frequency = 0;
        connectedTriples= new ArrayList<>();
        connectetionFrequency = new HashMap<>();
        partutAssignedPartition = -1;
    }

    public void addConnection(AnnomizedTriple triple , int frequency){
            if(connectetionFrequency.containsKey(triple)){
                connectetionFrequency.replace(triple, connectetionFrequency.get(triple) +frequency);
                return;
            }
        connectedTriples.add(triple);
        connectetionFrequency.put(triple,frequency);
    }

    public void createFragment(HashMap<Long, ArrayList<Triple>> pos, HashMap<Long, ArrayList<Triple>> osp, HashMap<Long, ArrayList<Triple>> sop , HashMap<Long, VertexGraph> vertexIndex){
        System.out.print("creating fragment ..");
        fragment = new Fragment(this) ;
        fragment.generateFromAnomTriple(pos , osp ,sop);
        fragment.filterToDist(2,vertexIndex);
        //TODO change the above value
    }
}
