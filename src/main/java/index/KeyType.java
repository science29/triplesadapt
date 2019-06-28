package index;

public class KeyType{
    public byte key[];

    public KeyType(byte[] keyB){
        key = keyB;
    }

    public KeyType(long keyL){

        key = longToBytes(keyL);
    }
    @Override
    public boolean equals(Object obj){
        KeyType sec = ((KeyType)obj);
        for(int i = 0 ; i < key.length ; i++ ){
            if(key[i] != sec.key[i])
                return false;
        }
        return true;
    }




    public static byte[] longToBytes(long l) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte)(l & 0xFF);
            l >>= 8;
        }
        return result;
    }

    public static long bytesToLong(byte[] b) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result |= (b[i] & 0xFF);
        }
        return result;
    }

}