package triple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;

public class MergeJoinResList {
    private ArrayList<Triple> abstractList;
    LinkedList<Integer> res;
    LinkedList<Integer> tempRes;
    int index = 0;
    int from = -1, to = -1;
    int currentIndex = 0;
    int forSortedOn; //means that this result should be projected on triples list sorted on the given forSortedIndex
    private int abstractIndex;
    private HashMap<Integer, Triple> prbeHashMap;

    ArrayList<Integer> skipBuffer = new ArrayList<>();

    int skipFrom = -1;
    int skipTo = -1;

    int count = 0;

    public MergeJoinResList(ArrayList<Triple> abstractList, int forSortedOn){
        this.forSortedOn = forSortedOn;
        this.abstractList = abstractList;
        res = new LinkedList<>();
        res.add(0);
        res.add(abstractList.size());
    }
    public MergeJoinResList(){
        //dummy constructor
    }

    public void reset(){
        index = 0;
        from = -1;
        to = -1;
        tempRes = new LinkedList<>();
    }


    public void resetIterate(){
        index = 0;
        to = 0;
        if(res.size() == 1)
            to = abstractList.size();
        abstractIndex = 0;
        resIt = res.listIterator();
        skipFrom = -1;
        skipTo = -1;
    }

    ListIterator<Integer> resIt;

    public Triple getNextLinked(){
        if(abstractIndex >= to){
            if(!resIt.hasNext())
                return null;
            checkSkipLinked();
            abstractIndex = resIt.next();
            if(resIt.hasNext()) {
                to = resIt.next();
            }
            else
                to = abstractList.size();
        }
        if(abstractIndex >= abstractList.size())
            return null;
        return abstractList.get(abstractIndex++);
    }



    public Triple getNextArr(){
        if(abstractIndex >= to){
            if(index >= res.size())
                return null;
            abstractIndex = res.get(index++);
            if(index < res.size())
                to = res.get(index++);
            else
                to = abstractList.size();
        }
        if(abstractIndex >= abstractList.size())
            return null;
        return abstractList.get(abstractIndex++);
    }


    public void done(){
        if(tempRes != null)
            res = tempRes;
        tempRes = null;
        //TODO
        //this.count = count;
        if(prevIndex != -1){
            count += ((abstractList.size()-1) - prevIndex);
            prevIndex = -1;
        }

    }

    public int count(){
        return count;
    }

    public int getNextIndex(){
        if(currentIndex > to) {
            if(index >= res.size())
                return -1;
            from = res.get(index);
            to = res.get(index + 1);
            index += 2;
            currentIndex = from;
            return currentIndex++;
        }
        return currentIndex++;
    }

    public void add(int from, int to) {
        if(tempRes == null)
            tempRes = new LinkedList<>();
        tempRes.add(from);
        tempRes.add(to);
    }


    int prevIndex = -1;

    public void addStep(int index) {
        tempRes.add(index);
        if(prevIndex == -1)
            prevIndex = index;
        else {
            count += (index - 1 - prevIndex);
            prevIndex = -1;
        }
    }

    public boolean contains(Integer var) {
        if(prbeHashMap == null) {
            prbeHashMap = new HashMap<>();
            resetIterate();
            while (true){
                Triple triple = getNextLinked();
                if(triple == null)
                    break;
                prbeHashMap.put(triple.triples[forSortedOn], triple);
            }
        }
        return prbeHashMap.containsKey(var);
    }


    public void skipCurrentIterationIndex() {
        //skipBuffer.add(abstractIndex-1);
        if(skipFrom == -1)
            skipFrom = abstractIndex;
        else
            skipTo = abstractIndex;
        ///count--;
    }

    private void checkSkipLinked() {
        if(skipFrom == -1)
            return;
        if(skipTo != -1) {
            resIt.add(skipFrom);
            resIt.add(skipTo);
            skipTo = -1;
        }
        else
            resIt.set(skipFrom);
        skipFrom = -1;
    }

    public void doneSkipping() {
        checkSkipLinked();
       /* if(skipBuffer.size() == 0)
            return;
        ArrayList newList = new ArrayList(skipBuffer.size()+res.size());
        for(Integer skip: skipBuffer){
         //TODO...
        }*/
    }

}
