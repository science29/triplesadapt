package triple;

public class MergeJoinResTotal {

    public Triple leftAbstract;
    public Triple rightAbstract;


   public MergeJoinResList leftRes;
   public MergeJoinResList rightRes;


    public MergeJoinResTotal(Triple leftAbstract, Triple rightAbstract, MergeJoinResList leftRes, MergeJoinResList rightRes) {
        this.leftAbstract = leftAbstract;
        this.rightAbstract = rightAbstract;
        this.leftRes = leftRes;
        this.rightRes = rightRes;
    }
}
