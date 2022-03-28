import triple.Prediacte;
import triple.Triple;

import java.util.ArrayList;

public class Minterm {


    ArrayList<Prediacte> prediactesList;

    public void addPredicate(Prediacte prediacte){
        prediactesList.add(prediacte);
    }

    public boolean isSatisfy(Triple tripel){
        for(int i= 0; i<prediactesList.size();i++){
            Prediacte prediacte = prediactesList.get(i);
                if(prediacte.constPredicate != tripel.triples[prediacte.variablePos])
                    return false;
        }
        if(prediactesList.size() == 0){
            System.out.println("error empty minterm is been evaluated ... returning false ..");
            return false;
        }
        return true;
    }


}
