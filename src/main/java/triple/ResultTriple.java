package triple;

import java.util.List;

public class ResultTriple {


    Triple triple;


    ResultTriple up;
    ResultTriple down;
    ResultTriple left;
    ResultTriple right;



    public ResultTriple(Triple triple){
        this.triple = triple;
    }

    public ResultTriple(List<Triple> result) {
        if(result.size() == 0)
            return;
        triple = result.get(0);
        ResultTriple prev = this;
        for(int i =1 ; i < result.size() ; i++){
            ResultTriple resultTriple = new ResultTriple(result.get(i));
            prev.down = resultTriple;
            prev = prev.down;
            if(this.down == null)
                this.down = resultTriple;
        }
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

    public Triple getTriple() {
        return triple;
    }

    public ResultTriple getLeft() {
        return left;
    }

    public ResultTriple getDown() {
        return down;
    }

    public ResultTriple getFarLeft() {
        if(left == null)
            return null;
        if(left.left == null)
            return left;
        return left.getFarLeft();
    }

    public ResultTriple getRight() {
        return right;
    }
}
