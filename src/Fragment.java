import triple.Triple;
import triple.TriplePattern;

import java.util.ArrayList;
import java.util.HashMap;

public class Fragment {

    /**
     * The best is to start from the anomized query graph and genrate a fragment for each annom. query, then we could decrese the size of the fragment if necessay
     */

    ArrayList<Triple> triples;
    //int size = -1;
    int frequency= -1;
    Minterm minterm;

    AnnomizedTriple annomizedTriple;


    ArrayList<Fragment> connectedFragments;
    ArrayList<Integer> connectionFrequency;

    public Fragment( Minterm minterm){
        this.minterm = minterm;
    }
    public Fragment(AnnomizedTriple annomizedTriple){
        this.annomizedTriple = annomizedTriple;
    }

    public void addTriple(Triple triple){
        triples.add(triple);
    }

    public int getSize(){
        return triples.size();
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public int getFrequency(){
        return this.frequency;
    }

    public ArrayList<Triple> getTriples() {
        return triples;
    }


    //curently only for the property  ...
    public void generateFromGraph(HashMap<Long,ArrayList<Triple> > POS){
        ArrayList<Prediacte> predicates = minterm.prediactesList;
        //currnetly only consider the 1st predicate
        Prediacte prediacte = predicates.get(0);
        triples = POS.get(prediacte.constPredicate);

        /*
        for(int i = 0 ; i< predicates.size() ; i++) {
            Prediacte prediacte = predicates.get(i);
            if(prediacte.variablePos == 1) {
                ArrayList<triple.triple> triples1 = POS.get(prediacte.constPredicate);
            }
        }*/
    }

    public void generateFromAnomTriple(HashMap<Long,ArrayList<Triple> > POS , HashMap<Long,ArrayList<Triple> > OSP , HashMap<Long,ArrayList<Triple> > SOP){
        ArrayList<Triple> ss = null ;
        ArrayList<Triple> pp = null ;
        ArrayList<Triple> oo = null ;
        if(annomizedTriple.triplePattern.triples[0] != TriplePattern.thisIsVariable)
            ss = SOP.get(annomizedTriple.triplePattern.triples[0]);
        if(annomizedTriple.triplePattern.triples[1] != TriplePattern.thisIsVariable)
            pp = POS.get(annomizedTriple.triplePattern.triples[1]);
        if(annomizedTriple.triplePattern.triples[2] != TriplePattern.thisIsVariable)
            oo = OSP.get(annomizedTriple.triplePattern.triples[2]);
        triples = Query.join(ss ,pp , oo);

    }

    public void  addConnectedFragment(Fragment fragment,int freq){
        if(connectedFragments == null)
            connectedFragments = new ArrayList<>();
        connectedFragments.add(fragment);
        if(connectionFrequency == null)
            connectionFrequency.add(freq);
    }

    public void filterToDist(int dist, HashMap<Long, VertexGraph> vertexIndex) {
        ArrayList<Triple> tempTripleFound = new ArrayList<>();
        for(int j=0 ;j<triples.size() ; j++){
            VertexGraph vs  = vertexIndex.get(triples.get(j).triples[0]);
            VertexGraph vo  = vertexIndex.get(triples.get(j).triples[2]);
            if((vs.dist < dist&&vs.dist !=-1) || (vo.dist < dist && vo.dist != -1) )
                tempTripleFound.add(triples.get(j));
        }
        System.out.println("size :"+triples.size()+" filtered: "+tempTripleFound.size());
        triples = tempTripleFound;
    }
}
