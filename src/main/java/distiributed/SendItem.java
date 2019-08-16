package distiributed;

import triple.ResultTriple;
import triple.TriplePattern2;

public class SendItem {

    public int queryNo;
    public int [] tripleNo;
    public ResultTriple resultTriple;

    public SendItem(int queryNo, TriplePattern2 triplePattern2, ResultTriple resultTriple) {
        this.queryNo = queryNo;
        this.tripleNo = triplePattern2.getTriples();
        this.resultTriple = resultTriple;
    }

    public SendItem() {

    }

    public byte[] getBytes() {
        xxxx
    }
}
