package triple;

import java.util.ArrayList;
import java.util.List;

public class ResultTriple {


    Triple triple;


    ResultTriple down;
    ResultTriple left;
    ResultTriple right;



    ResultTriple extraDown;
    private int borderIndex = -1;
    public boolean cached = false;

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

    public ResultTriple() {
    }


    public ResultTriple getExtraDown() {
        return extraDown;
    }


    public static ResultTriple getDummyBorder(int borderIndex) {
        ResultTriple resultTriple = new ResultTriple();
        resultTriple.setMissingBorder(borderIndex);
        return resultTriple;
    }

    public void setLeft(ResultTriple resultTriple){
        left = resultTriple;
    }

    public void setRight(ResultTriple resultTriple){
        right = resultTriple;
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

    public void setBorder(int borderIndex) {
        this.borderIndex = borderIndex;
    }

    public boolean isBorder(int myIndex) {
        return borderIndex == myIndex;
    }

    public boolean requireBorder() {
        return borderIndex >= 0;
    }

    public void setRequireBorder(boolean left) {
        if(borderIndex >= 0)
            return;
        if(left)
            borderIndex = 4;
        else
            borderIndex = 5;
    }

    public void setExtraDown(ResultTriple resultTriple2) {
        this.extraDown = resultTriple2;
    }

    public boolean requireBorderLeft() {
        return borderIndex == 4 || borderIndex == 0;
    }

    public boolean requireBorderRight() {
        return borderIndex == 5 || borderIndex == 2;
    }

    public void setMissingBorder(int myIndex) {
        if(myIndex == 0)
            borderIndex = 6;
        else
            borderIndex = 7;
    }

    public boolean isMissingBorder() {
        return borderIndex >= 6;
    }

    public ArrayList<Triple> getList() {
        ArrayList<Triple> res = new ArrayList<Triple>();
        ResultTriple pointer = this;
        while(pointer != null){
            res.add(pointer.triple);
            pointer = pointer.down;
        }
        return res;
    }
}
