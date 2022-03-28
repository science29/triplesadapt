package QueryStuff;

import triple.TriplePattern2;

import java.util.HashMap;

public class QueryCache {


    private final HashMap<PropertiesPair , PatternPair> cache;
    private final PropertiesPair tempkey;

    public QueryCache() {
        this.cache = new HashMap<>();
        tempkey = new PropertiesPair(0,0);
    }


    public void addQuery(PropertiesPair key, PatternPair patternPair){
        cache.put(key , patternPair);
    }



    public PatternPair getStartCachedPattern(int p1 , int p2){
        tempkey.set(p1,p2);
        return  cache.get(tempkey);
    }


    public class PropertiesPair {
         int p1;
         int p2;

        public PropertiesPair(int p1, int p2) {
            this.p1 = p1;
            this.p2 = p2;
        }

        public void set(int p1, int p2) {
            this.p1 = p1;
            this.p2 = p2;

        }
    }


    public class PatternPair {
        public TriplePattern2 pattern1;
        public TriplePattern2 pattern2;

    }
}
