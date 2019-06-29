package triple;

public class ResultTriple {


    Triple triple;


    ResultTriple up;
    ResultTriple down;
    ResultTriple left;
    ResultTriple right;



    public ResultTriple(Triple triple){
        this.triple = triple;
    }

    public void setLeft(ResultTriple resultTriple){
        left = resultTriple;
    }

    public void setRight(ResultTriple resultTriple){
        right = resultTriple;
    }

    public void setUp(ResultTriple resultTriple){
        up = resultTriple;
    }

    public void setDown(ResultTriple resultTriple){
        down =  resultTriple;
    }
}
