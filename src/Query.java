import java.util.ArrayList;
import java.util.HashMap;

public class Query {
    public ArrayList<TriplePattern> triplePatterns;
    public int queryFrquency;
    public ArrayList<TriplePattern> simpleAnswer;
    public ArrayList<ArrayList<TriplePattern>> fullAnswer;

    private HashMap<TriplePattern, ArrayList<Triple>> answerMap;
    public String SPARQL;

    public String partitionString = "";
    public String answerString = "";

    public Query(ArrayList<TriplePattern> triplePattern, int queryFrquency, ArrayList<TriplePattern> simpleAnswer) {
        this.triplePatterns = triplePattern;
        this.queryFrquency = queryFrquency;
        this.simpleAnswer = simpleAnswer;
    }

    public void findStringTriple(HashMap<Long, String> reverseDictionary) {
        for (int i = 0; i < triplePatterns.size(); i++) {
            triplePatterns.get(i).findStringTriple(reverseDictionary);
        }
    }

    public void setQuerySPARQL(HashMap<Long, String> reverseDictionary, HashMap<String, String> prefixIndex, HashMap<Long, VertexGraph> verticies) {
        SPARQL = "";
        String predicates = "";
        ArrayList<String> varsList = new ArrayList<>();
        for (int i = 0; i < triplePatterns.size(); i++) {
            String s, p, o;
            if (triplePatterns.get(i).triples[0] != TriplePattern.thisIsVariable) {
                s = reverseDictionary.get(triplePatterns.get(i).triples[0]);
                s = getFullStringElem(s, prefixIndex);
            } else {
                s = "?x" + -1 * triplePatterns.get(i).variablesIndex.get(triplePatterns.get(i).fixedTriples[0]);
                varsList.add(s);
            }
            if (triplePatterns.get(i).triples[1] != TriplePattern.thisIsVariable) {
                p = reverseDictionary.get(triplePatterns.get(i).triples[1]);
                p = getFullStringElem(p, prefixIndex);
            } else
                p = "?x" + -1 * triplePatterns.get(i).variablesIndex.get(triplePatterns.get(i).fixedTriples[1]);
            if (triplePatterns.get(i).triples[2] != TriplePattern.thisIsVariable) {
                o = reverseDictionary.get(triplePatterns.get(i).triples[2]);
                o = getFullStringElem(o, prefixIndex);
            } else
                o = "?x" + -1 * triplePatterns.get(i).variablesIndex.get(triplePatterns.get(i).fixedTriples[2]);

            predicates += s + " " + p + " " + o;
            if (i + 1 < triplePatterns.size())
                predicates += ".";
        }
        String vars = "";
        for (int k = 0; k < varsList.size(); k++) {
            vars = vars + " " + varsList.get(k);
        }
        SPARQL = "select " + vars + " where {" + predicates + "}";
        for (int i = 0; i < simpleAnswer.size(); i++) {
            long v = simpleAnswer.get(i).triples[0];
            answerString = answerString + "    " + (reverseDictionary.get(simpleAnswer.get(i).triples[0]));
            partitionString = partitionString + " " + verticies.get(v).partitionNumber;
        }
        // vars = "?x1";
    }

    private String getFullStringElem(String elem, HashMap<String, String> prefixIndex) {
        if (elem.contains("http"))
            return elem;
        if (!elem.contains("<") && elem.contains(":")) {
            String key = elem.split(":")[0];
            String pref = prefixIndex.get(key);
            String newPref = pref.replace(">", elem.split(":")[1].trim() + ">");
            return newPref;
        }
        if (elem.contains("<")) {
            String pref = prefixIndex.get("base");
            String elemT;
            elemT = elem.replace("<", "");
            elemT = elemT.replace(">", "");
            String newPref = pref.replace(">", elemT.trim() + ">");
            return newPref;
        }
        return elem;
    }

    public static ArrayList<Triple> join(ArrayList<Triple> ss, ArrayList<Triple> pp, ArrayList<Triple> oo) {
        ArrayList<Triple> r1 = null;
        ArrayList<Triple> r2 = null;
        int i1 = 0, i2 = 0;
        if (ss != null)
            r1 = ss;
        else if (pp != null) {
            r1 = pp;
            i1 = 1;
        } else {
            r1 = oo;
            i1 = 2;
            return r1;
        }

        if (ss != null) { // means : r1 = ss
            if (pp != null) {
                r2 = pp;
                i2 = 1;
            } else if (oo != null) {
                r2 = oo;
                i2 = 2;
            } else
                return r1;
        } else if (pp != null && oo != null) {
            r2 = oo;
            i2 = 2;
        }

        if (r2 == null || r1 == null)
            return r1;

        ArrayList res = join(r1, r2, i1, i2);

        return res;

    }


    public static ArrayList<Triple> join(ArrayList<Triple> t1, ArrayList<Triple> t2, int i1, int i2) {
        long match1 = t1.get(0).triples[i1];
        long match2 = t2.get(0).triples[i2];
        ArrayList<Triple> res = new ArrayList<>();
        for (int j = 0; j < t1.size(); j++) {
            if (t1.get(j).triples[i2] == match2) {
                res.add(t1.get(j));
            }
        }
        for (int j = 0; j < t2.size(); j++) {
            if (t2.get(j).triples[i1] == match1) {
                res.add(t2.get(j));
            }
        }

        return res;
    }


    public void findChainQueryAnswer(HashMap<String, ArrayList<Triple>> OPxP, HashMap<String, ArrayList<Triple>> opS) {
        fullAnswer = new ArrayList<>();
        HashMap<TriplePattern, Triple> answer = new HashMap<>();
        answerMap = new HashMap<>();
        //first find triple pattern that has po fixed
        TriplePattern triplePattern1 = null, triplePattern2 = null;
        for (int i = triplePatterns.size() - 1; i >= 0; i--) {
            triplePattern1 = triplePatterns.get(i);
            if (!TriplePattern.isVariable(triplePattern1.triples[1]) && !TriplePattern.isVariable(triplePattern1.triples[2]) && TriplePattern.isVariable(triplePattern1.triples[0]))
                break;
            triplePattern1 = null;
        }
        if (triplePattern1 == null) {
            System.err.println("Error finding po fixed in chain query");
            return;
        }

        //now look for the other p
        for (int i = triplePatterns.size() - 1; i >= 0; i--) {
            triplePattern2 = triplePatterns.get(i);
            if (triplePattern1.triples[0] == triplePattern2.triples[2])
                break;
            triplePattern2 = null;
        }
        //now build the index key OPP
        String key = triplePattern1.triples[2] + "" + triplePattern1.triples[1] + "" + triplePattern2.triples[1];
        ArrayList<Triple> list = OPxP.get(key);
        //now move to all triples in list
        for (int i = 0; i < list.size() + 1; i += 2) {

            Triple triple1 = list.get(i);
            answer.put(triplePattern1, triple1);
            Triple triple2 = list.get(i + 1);
            answer.put(triplePattern2, triple2);
            long next_o = triple2.triples[0];
            TriplePattern nextTripelPatern = getNextTriplePattern(triplePattern2, 0);
            long next_p = nextTripelPatern.triples[1];
            key = next_o + "" + next_p;
            ArrayList<Triple> list2 = opS.get(key);
            findDeepAnswer(nextTripelPatern, triple2, opS);
            answer.put(nextTripelPatern, triple2);

        }


    }

    private boolean findDeepAnswer(TriplePattern triplePattern, Triple triple, HashMap<String, ArrayList<Triple>> opS) {
        long next_o = triple.triples[0];
        TriplePattern nextTripelPatern = getNextTriplePattern(triplePattern, 0);
        if (nextTripelPatern == null)
            return true;
        if (!triplePattern.matches(triple))
            return false;
        long next_p = nextTripelPatern.triples[1];
        String key = next_o + "" + next_p;
        ArrayList<Triple> list = opS.get(key);
        if(list == null)
            return false;
        boolean found = false;
        for (int i = 0; i < list.size(); i++) {
            Triple triple2 = list.get(i);
            boolean res = findDeepAnswer(nextTripelPatern, triple2, opS);
            if (res) {
                found = true;
                ArrayList<Triple> answerList = answerMap.get(triplePattern);
                if (answerList == null) {
                    answerList = new ArrayList<>();
                    answerMap.put(triplePattern, answerList);
                }
                answerList.add(triple);//xx
            }
        }
        return found;
    }


    private HashMap<TriplePattern , TriplePattern> seenPatterns = new HashMap<>();
    private TriplePattern getNextTriplePattern(TriplePattern triplePattern, int index) {
        //find the triple pattern that is connecte to triplePattern from index i
        TriplePattern firstNotSeen = null;
        for(int i=0; i < triplePatterns.size() ; i++){
            TriplePattern candiatePattern = triplePatterns.get(i);
            if(!seenPatterns.containsKey(candiatePattern)) {
                if(firstNotSeen == null)
                    firstNotSeen = candiatePattern;
                if (candiatePattern.triples[index] == triplePattern.triples[index]) {
                    seenPatterns.put(candiatePattern, candiatePattern);
                    return candiatePattern;
                }
            }

        }
        return firstNotSeen;
    }


    public void parseSparqlChain(String spaql, HashMap<String, Long> dictionary) {
        /*" select  ?x1 ?x3 ?x5 ?x7 where " +
                "{?x1 <http://mpii.de/yago/resource/describes> ?x3.?x3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?x5." +
                "?x5 <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?x7." +
                "?x7 <http://mpii.de/yago/resource/isPartOf> <http://mpii.de/yago/resource/wordnet_transportation_system_104473432>} ";*/
        triplePatterns = new ArrayList<>();
        String s = spaql.split("\\{")[1];
        s = s.replace("}", "");
        String[] patterns = s.split(".");
        for (int j = 0; j < patterns.length; j++) {
            String[] x = patterns[j].split(" ");

            long ss, pp, oo;
            try {
                if (x[0].startsWith("?"))
                    ss = TriplePattern.thisIsVariable;
                else
                    ss = dictionary.get(x[0]);
                if (x[1].startsWith("?"))
                    pp = TriplePattern.thisIsVariable;
                else
                    pp = dictionary.get(x[1]);
                if (x[2].startsWith("?"))
                    oo = TriplePattern.thisIsVariable;
                else
                    oo = dictionary.get(x[2]);
                TriplePattern triplePattern = new TriplePattern(ss, pp, oo);
                triplePatterns.add(triplePattern);
            } catch (Exception e) {
                System.err.println("error parsing query");
                System.exit(1);
            }
        }
    }



    /*public void parseSparqlChain(String spaql){
        " select  ?x1 ?x3 ?x5 ?x7 where " +
                "{?x1 <http://mpii.de/yago/resource/describes> ?x3.?x3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?x5." +
                "?x5 <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?x7." +
                "?x7 <http://mpii.de/yago/resource/isPartOf> <http://mpii.de/yago/resource/wordnet_transportation_system_104473432>} ";

        
    }*/

}
