package distiributed;

import index.Dictionary;
import triple.ResultTriple;
import triple.Triple;

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
        buildSerial(resultTriple.getFarLeft() , intList, null);
        intList.add(triple[0]);
        intList.add(triple[1]);
        intList.add(triple[2]);
        intList.add(queryNo);
        byte [] data = new byte[(intList.size()+1)*4];
        int bIndex = 0;
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


    public static void buildSerial(ResultTriple resultTriple, ArrayList<Integer> intList, Dictionary reverseDictionary){
        intList.add(resultTriple.getTriple().triples[0]);
        intList.add(resultTriple.getTriple().triples[1]);
        intList.add(resultTriple.getTriple().triples[2]);
        if(reverseDictionary != null) {
            String str = reverseDictionary.get(resultTriple.getTriple().triples[0]) + " " + reverseDictionary.get(resultTriple.getTriple().triples[1]) + " " + reverseDictionary.get(resultTriple.getTriple().triples[2]);
            System.out.print(str + " . ");
        }
        if(resultTriple.getRight() != null) {
            System.out.println("going right");
            intList.add(-1);
            buildSerial(resultTriple.getRight(), intList, reverseDictionary);
        }
        if(resultTriple.getDown() != null) {
            System.out.println("going down");
            buildSerial(resultTriple.getDown(), intList, reverseDictionary);
        }
        intList.add(0);
    }



    public static void buildSerial3(ResultTriple resultTriple, ArrayList<Integer> intList , int direction ,Dictionary reverseDictionary){
        if(resultTriple == null) {
            return;
        }
        if(direction == 0)
            intList.add(0);
        intList.add(resultTriple.getTriple().triples[0]);
        intList.add(resultTriple.getTriple().triples[1]);
        intList.add(resultTriple.getTriple().triples[2]);
        if(direction == 0) {
            intList.add(-2);
            buildSerial3(resultTriple.getRight() , intList , -2,reverseDictionary);
            intList.add(-3);
            buildSerial3(resultTriple.getLeft(), intList , -3,reverseDictionary);
            buildSerial3(resultTriple.getDown(), intList, 0, reverseDictionary);
            intList.add(0);
        }else{
            if(resultTriple.getDown() != null){
                intList.add(-1);
                buildSerial3(resultTriple.getDown(), intList, -1, reverseDictionary);
                intList.add(-1);
            }
            if(direction == -2)
                buildSerial3(resultTriple.getRight() , intList , direction,reverseDictionary);
            if(direction == -3)
                buildSerial3(resultTriple.getLeft() , intList , direction,reverseDictionary);
        }


    }

    public static void buildFromSerial3(ArrayList<Integer> intList ){
        ResultTriple head = null, pointer = null , movingHead = null;
        for(int i = 0 ; i < intList.size() ; i++){
            Triple triple = new Triple(intList.get(i++) , intList.get(i++) ,intList.get(i++));
            ResultTriple resultTriple2 = new ResultTriple(triple);
            if(head == null){
                head = resultTriple2;
                pointer = resultTriple2;
                movingHead = resultTriple2;
                i++;
            }else {
                int dir = intList.get(i++);
                if (dir == 0) {
                    movingHead.setDown(resultTriple2);
                    movingHead = resultTriple2;
                }
                if (dir == -1) {
                    pointer.setDown(resultTriple2);
                    pointer = resultTriple2;
                }
                if (dir == -2) {
                    pointer.setRight(resultTriple2);
                    pointer = resultTriple2;
                }
                if (dir == -3) {
                    pointer.setLeft(resultTriple2);
                    pointer = resultTriple2;
                }
                if(intList.get(i+1) == 0){
                   pointer =  movingHead ;
                   i++;
                }
            }
        }
    }



    public static void buildSerial2(ResultTriple resultTriple, ArrayList<Integer> intList ,Dictionary reverseDictionary){

        ResultTriple vResultTriple = resultTriple;
        if (vResultTriple == null)
            return;
        do {
            ResultTriple hResultTriple = vResultTriple.getFarLeft();
            while (hResultTriple != null) {
                ResultTriple pHTriple = hResultTriple;
                boolean moreThanOne = false;
                do {
                    Triple triple = pHTriple.getTriple();
                    String str = reverseDictionary.get(triple.triples[0]) + " " + reverseDictionary.get(triple.triples[1]) + " " + reverseDictionary.get(triple.triples[2]);
                    System.out.print(str + " . ");
                    intList.add(resultTriple.getTriple().triples[0]);
                    intList.add(resultTriple.getTriple().triples[1]);
                    intList.add(resultTriple.getTriple().triples[2]);

                    pHTriple = pHTriple.getDown();
                    if(moreThanOne) {
                        System.out.println();
                    }
                    moreThanOne = true;
                }while(pHTriple != null && vResultTriple != hResultTriple);
                hResultTriple = hResultTriple.getRight();
            }
            intList.add(0);
            vResultTriple = vResultTriple.getDown();
        }while (vResultTriple != null);


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
