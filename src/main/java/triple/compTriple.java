package triple;

public  class compTriple {


    private static final int BYTES_PER_ELEMENT = 4 ;
    private static final int SUBJECT_INDEX = 0 ;
    private static final int PREDICATE_INDEX = 4;
    private static final int OBJECT_INDEX = 8;

    public static byte[] getNewTriple(){
        return new byte[BYTES_PER_ELEMENT * 3];
    }


    public static byte[] getSubject(byte[] triple){
        byte [] s = new byte[BYTES_PER_ELEMENT];
        for(int i = 0; i < s.length ; i++){
            s[i] = triple[SUBJECT_INDEX+i];
        }
        return s;
    }

    public static byte[] getPredicate(byte[] triple){
        byte [] s = new byte[BYTES_PER_ELEMENT];
        for(int i = 0; i < s.length ; i++){
            s[i] = triple[PREDICATE_INDEX+i];
        }
        return s;
    }

    public static byte[] getObject(byte[] triple){
        byte [] s = new byte[BYTES_PER_ELEMENT];
        for(int i = 0; i < s.length ; i++){
            s[i] = triple[OBJECT_INDEX+i];
        }
        return s;
    }
}
