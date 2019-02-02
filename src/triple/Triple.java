package triple;

public class Triple {
    final static int Subject = 77;
    final static int Object = 88;
    final static int Property = 99;

    public long triples[]= new long[3];
    //int varaibles[] = new int[3];

    public Triple(long s ,long p , long o){
        triples[0] = s ;
        triples[1] = p;
        triples[2] = o;
    }
}
