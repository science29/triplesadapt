package triple;

public class Vertex {


    public long v;
    public final long e;
    public int weight1;
    public int weight2;
    public boolean isBorder;

    public Vertex(long v, long e) {
        this.v = v;
        this.e = e;
        weight1 = 1;
        weight2 = 1;
        isBorder = false;
    }

}
