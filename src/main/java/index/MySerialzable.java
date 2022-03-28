package index;

import triple.Vertex;

public interface MySerialzable {

     String serialize();
    Vertex deSerialize(String res);

}
