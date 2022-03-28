package triple;

import index.MySerialzable;

public class Vertex implements MySerialzable {


    public int v;
    public  int e;
    public int weight1;
    public int weight2;
    public boolean isBorder;

    public Vertex(int v, int e) {
        this.v = v;
        this.e = e;
        weight1 = 1;
        weight2 = 1;
        isBorder = false;
    }


    public String serialize(){
        StringBuilder res = new StringBuilder(v+"");
        res.append(",");
        res.append(e);
        res.append(",");
        res.append(weight1);
        res.append(",");
        res.append(weight2);
        res.append(",");
        if(isBorder)
            res.append(1);
        else
            res.append(0);
        return res.toString();
    }

    public Vertex deSerialize(String res){
        Vertex vertex = new Vertex(0,0);
        String [] resArr = res.split(",");
        vertex.v = Integer.valueOf(resArr[0]);
        vertex.e = Integer.valueOf(resArr[1]);
        vertex.weight1 = Integer.valueOf(resArr[2]);
        vertex.weight2 = Integer.valueOf(resArr[3]);
        vertex.isBorder= Integer.valueOf(resArr[4]) == 1;
        return vertex;
    }

}
