package optimizer.stat;

import index.Dictionary;
import index.IndexesPool;
import index.MyHashMap;
import triple.Triple;
import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ClassesStat {
    private final Integer typeID;
    private final TriplePattern2.WithinIndex withinIndex;
    //private final String typePrefixVal;
    MyHashMap<Integer, ArrayList<Triple>> POS;
    MyHashMap<Integer, ArrayList<Triple>> OPS;
    MyHashMap<Integer, ArrayList<Triple>> SPO;
    Dictionary dictionary;

    HashMap<Integer,ClassesRel> classesMap = new HashMap<>();
    TriplePattern2 triplePattern2 = new TriplePattern2();



    MyHashMap<Integer, ArrayList<Triple>> SPOClasses = new MyHashMap("SPOClasses");
    MyHashMap<Integer, ArrayList<Triple>> OPSClasses = new MyHashMap("OPSClasses");
    MyHashMap<Integer, ArrayList<Triple>> POSClasses = new MyHashMap("POSClasses");


    HashMap<String , ClassesTriple> classesTripleMap = new HashMap<>();



    public ClassesStat(IndexesPool indexesPool, Integer typeID){
        this.typeID = typeID;
       // this.typePrefixVal = typePrefixVal;
        this.POS = indexesPool.getIndex(IndexesPool.POs);
        this.OPS = indexesPool.getIndex(IndexesPool.OPs);
        this.SPO = indexesPool.getIndex(IndexesPool.SPo);
        this.withinIndex = triplePattern2.getWithinIndexInstance();

    }
/*
     public void process() {
        ArrayList<Triple> list = POS.get(dictionary.get(typeLiteral,typePrefixVal));
        Integer prevO = null;
        ClassesRel classesRel = null;
        for(Triple triple : list){
            if(prevO == null || triple.triples[2] != prevO) {
                classesRel = new ClassesRel(triple.triples[2]);
                classesMap.put(triple.triples[2], classesRel);
            }
            processToClassRel(classesRel, triple);
            prevO = triple.triples[2];
        }

    }

   public void work(){
        ArrayList<Triple> list = POS.get(dictionary.get("type"));xxx;
        Integer prevO = null;
        Integer prevS = null;
        int count = 0;
        
        for(Triple triple : list){
            if(triple.triples[2] == prevO){
                count++;
            }else {
                ClassesRel classesRel = new ClassesRel(prevO, count);
                ArrayList<Triple> listSO = OPS.get(prevS);
                Integer prevP = null;
                for(Triple triple1 : listSO){
                    if(prevP != triple1.triples[1])
                        classesRel.addPredicateStat(triple1.triples[1]);
                    prevP = triple1.triples[1];
                }
                prevP = null;
                ArrayList<Triple> listSS = SPO.get(prevS);
                for(Triple triple1 : listSS){
                    if(prevP != triple1.triples[1])
                        classesRel.addPredicateStat(triple1.triples[1]);
                    prevP = triple1.triples[1];
                }
            }
            prevO = triple.triples[2];
            prevS = triple.triples[0];
        }

    }


    private void processToClassRel(ClassesRel classesRel, Triple triple){

        ArrayList<Triple> listSO = OPS.get(triple.triples[0]);
        Integer prevP = null;
        for(Triple triple1 : listSO){
            if(prevP == null || prevP != triple1.triples[1])
                classesRel.addPredicateStat(triple1.triples[1]);
            prevP = triple1.triples[1];
        }
        prevP = null;
        ArrayList<Triple> listSS = SPO.get(triple.triples[0]);
        for(Triple triple1 : listSS){
            if(prevP==null || prevP != triple1.triples[1])
                classesRel.addPredicateStat(triple1.triples[1]);xx
            prevP = triple1.triples[1];
        }

    }
*/



    public void addToStat(Triple tripleOrg , boolean fullStore){
        Triple triple = replaceIDwithClass(tripleOrg);
        if(triple == null)
            return;
        ClassesTriple clsTriple = classesTripleMap.get(triple.triples[0]+""+triple.triples[1]+""+triple.triples[2]);
        if(clsTriple == null){
            clsTriple = new ClassesTriple(triple);
            classesTripleMap.put(triple.triples[0]+""+triple.triples[1]+""+triple.triples[2], clsTriple);
            ArrayList<Triple> list = new ArrayList<>();
            list.add(triple);
            SPOClasses.put(triple.triples[0], list);
            OPSClasses.put(triple.triples[2], list);
            POSClasses.put(triple.triples[1], list);
        }
        if(fullStore)
            clsTriple.addTriple(tripleOrg);
        clsTriple.count++;
    }

    public void done(){
        SPOClasses.sort(1,2);
        OPSClasses.sort(1,0);
        POSClasses.sort(2,0);

        Iterator<Map.Entry<String, ClassesTriple>> iterator = classesTripleMap.entrySet().iterator();
        while (iterator.hasNext()){
            try {
                Map.Entry<String, ClassesTriple> pair = iterator.next();
                ClassesTriple classesTriple = pair.getValue();
                ArrayList<Triple> list_t = OPS.get(classesTriple.tripleAbstract.triples[0], typeID, 1, withinIndex);
                int sCount = list_t.size();//OPS.get(repTriple.triples[0]).size();
                list_t = OPS.get(classesTriple.tripleAbstract.triples[2], typeID, 1, withinIndex);
                int oCount = list_t.size();//OPS.get(repTriple.triples[2]).size();
                classesTriple.totalSClass = sCount;
                classesTriple.totalOClass = oCount;
                classesTriple.dupSortOnBoth();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }


    private Triple replaceIDwithClass(Triple t){
        TriplePattern2 triplePattern2 = new TriplePattern2();
        TriplePattern2.WithinIndex withinIndex = triplePattern2.getWithinIndexInstance();
        ArrayList<Triple> list = SPO.get(t.triples[0], typeID, 1 , withinIndex);
        if(list == null)
            return null;
        Integer subjectType = list.get(withinIndex.index).triples[2];

        list = SPO.get(t.triples[2], typeID,1, withinIndex);
        if(list == null)
            return null;
        Integer ObjectType = list.get(withinIndex.index).triples[2];


        Triple newTriple = new Triple(subjectType,t.triples[1],ObjectType);
        return newTriple;

    }

    public ArrayList<Triple> getTriples(Triple triple) {
        ClassesTriple classesTriple = classesTripleMap.get(triple.triples[0] + "" + triple.triples[1] + "" + triple.triples[2]);
        return classesTriple.triples;
    }


    public ClassesTriple getClassesTriple(Triple triple) {
        ClassesTriple classesTriple = classesTripleMap.get(triple.triples[0] + "" + triple.triples[1] + "" + triple.triples[2]);
        return classesTriple;
    }
}
