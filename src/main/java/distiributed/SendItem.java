package distiributed;

import triple.ResultTriple;
import triple.Triple;
import triple.TriplePattern2;

import java.util.ArrayList;

public class SendItem {

    public int queryNo;
    public int [] triple;
    public ResultTriple resultTriple;

    public SendItem(int queryNo, TriplePattern2 triplePattern2, ResultTriple resultTriple) {
        this.queryNo = queryNo;
        this.triple = triplePattern2.getTriples();
        this.resultTriple = resultTriple;
    }

    public SendItem() {

    }

    public byte[] getBytes() {
        ArrayList<Integer> intList = new ArrayList<>();
        buildSerial(resultTriple , intList);
        byte [] data = new byte[(intList.size()+1)*4];
        int bIndex = 0;
        intList.add(queryNo);
        for(int i = 0; i < intList.size() ; i++){
            int val = intList.get(i);
            data[bIndex++] = (byte) (intList.get(i) >> 24);
            data[bIndex++] = (byte) (intList.get(i) >> 16);
            data[bIndex++] = (byte) (intList.get(i) >> 8);
            data[bIndex++] = (byte) (intList.get(i).intValue() /*>> 0*/);
            data[i] = intList.get(i).byteValue();
        }
        return data;
    }

    public static SendItem fromByte(byte [] data){

    }



    @Deprecated
    private void buildSerialT(ResultTriple resultTriple , ArrayList<Integer> intList ){
        intList.add(resultTriple.getTriple().triples[0]);
        intList.add(resultTriple.getTriple().triples[1]);
        intList.add(resultTriple.getTriple().triples[2]);
        int returnIndex = intList.size();
        if(resultTriple.getRight() != null)
            buildSerialT(resultTriple.getRight() , intList);
        intList.add(-1*returnIndex);
        if(resultTriple.getDown() != null)
            buildSerialT(resultTriple.getDown() , intList);
        intList.add(0);
    }


    private void buildSerial(ResultTriple resultTriple , ArrayList<Integer> intList ){
        intList.add(resultTriple.getTriple().triples[0]);
        intList.add(resultTriple.getTriple().triples[1]);
        intList.add(resultTriple.getTriple().triples[2]);
        if(resultTriple.getRight() != null) {
            intList.add(-1);
            buildSerial(resultTriple.getRight(), intList);
        }
        if(resultTriple.getDown() != null)
            buildSerial(resultTriple.getDown() , intList);
        intList.add(0);
    }


    private ResultTriple buildFromSerial(ArrayList<Integer> intList , int index){
        Triple triple = new Triple(intList.get(index++) , intList.get(index++) ,intList.get(index++));
        ResultTriple resultTriple2 = new ResultTriple(triple);
        if(intList.get(index++) < 0)
            resultTriple2.setRight(buildFromSerial(intList , index));
        else if(intList.get(index++)  != 0)
            resultTriple2.setDown( buildFromSerial(intList , index));
        else
            return resultTriple2;
        return null;
    }

    @Deprecated
    private void buildFromSerialT(ArrayList<Integer> intList){
        ResultTriple resultTriple = null;
        int index = 0;
        for(int i = 0; i < intList.size() ; i++){
            Triple triple = new Triple(intList.get(i++) , intList.get(i++) ,intList.get(i++));
            resultTriple = new ResultTriple(triple);
        }
    }
}
