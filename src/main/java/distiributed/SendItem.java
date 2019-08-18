package distiributed;

import triple.ResultTriple;
import triple.Triple;
import triple.TriplePattern2;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;

public class SendItem {

    public int queryNo;
    public int [] triple;
    public ResultTriple resultTriple;

    public SendItem(int queryNo, int [] triple, ResultTriple resultTriple) {
        this.queryNo = queryNo;
        this.triple = triple;
        this.resultTriple = resultTriple;
    }

    public SendItem() {

    }

    public byte[] getBytes() {
        ArrayList<Integer> intList = new ArrayList<>();
        buildSerial(resultTriple , intList);
        byte [] data = new byte[(intList.size()+1)*4];
        int bIndex = 0;
        intList.add(triple[0]);
        intList.add(triple[1]);
        intList.add(triple[2]);
        intList.add(queryNo);
        for(int i = 0; i < intList.size() ; i++){
            data[bIndex++] = (byte) (intList.get(i) >> 24);
            data[bIndex++] = (byte) (intList.get(i) >> 16);
            data[bIndex++] = (byte) (intList.get(i) >> 8);
            data[bIndex++] = (byte) (intList.get(i).intValue() /*>> 0*/);
            data[i] = intList.get(i).byteValue();
        }
        return data;
    }


    public static SendItem fromByte(byte [] data){
        IntBuffer intBuf =
                ByteBuffer.wrap(data)
                        .order(ByteOrder.BIG_ENDIAN)
                        .asIntBuffer();
        int[] intArray = new int[intBuf.remaining()];
        intBuf.get(intArray);

        ResultTriple resultTriple = buildFromSerial(intArray , 0);
        int [] triple = {intArray[intArray.length-4] , intArray[intArray.length-3] , intArray[intArray.length-2]};
        SendItem sendItem = new SendItem(intArray[intArray.length-1] ,triple  ,resultTriple );
        return sendItem;
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


    private static ResultTriple buildFromSerial(int [] intArr , int index){
        Triple triple = new Triple(intArr[index++] , intArr[index++] ,intArr[index++]);
        ResultTriple resultTriple2 = new ResultTriple(triple);
        if(intArr[index++] < 0)
            resultTriple2.setRight(buildFromSerial(intArr , index));
        else if(intArr[index++]  != 0)
            resultTriple2.setDown( buildFromSerial(intArr , index));
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
