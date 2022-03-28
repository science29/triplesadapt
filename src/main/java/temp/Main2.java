package temp;

import QueryStuff.*;
import index.Dictionary;
import index.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import triple.Triple;
import triple.Vertex;
import util.temp2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.regex.PatternSyntaxException;


@Deprecated
public class Main2 {

    private static final int BASE_PREDICATE_CODE = 1000000000;
    private VertexGraphIndex graph = new VertexGraphIndex("graph");

    private HashMap<Integer, ArrayList<Triple>> tripleGraph = new HashMap();//duplicate with graph , remove one!
    private ArrayList<Integer> vertecesID = new ArrayList();
    private HashMap<Integer, VertexGraph> verticies = new HashMap<Integer, VertexGraph>();
    ;
    private Dictionary dictionary = new Dictionary("dictionary");
    //private HashMap<Integer, String> reverseDictionary = new HashMap();
    Dictionary reverseDictionary = dictionary;
    private HashMap<Integer, ArrayList<VertexGraph>> distanceVertex;
    private HashMap<Integer, Integer> PredicatesAsSubject;
    private int edgesCount = 0;
    private static final int PARTITION_COUNT = 2;

    private HashMap<Integer, ArrayList<Triple>> POS;
    private MyHashMap<Integer, ArrayList<Triple>> SPO;
    private MyHashMap<Integer, ArrayList<Triple>> OPS;
    private MyHashMap<String, ArrayList<Triple>> sp_O;
    private MyHashMap<String, ArrayList<Triple>> op_S;
    private MyHashMap<String, ArrayList<Triple>> SPxP = new MyHashMap("SPxP");
    private MyHashMap<String, ArrayList<Triple>> OPxP = new MyHashMap("OPxP", new IndexType(1, 0, 0));

    private HashMap<Integer, VertexGraph> toBeCheckedVertexes; // this is the list of non boraders vertex that need to be checked if they have been written before the border ones

    private int edgesWritten = 0;
    private HashMap<Integer, HashMap<Integer, Vertex>> graphMap = new HashMap();
    private HashMap<Integer, String> tempIntToStringMap = new HashMap();
    double progressRatio = 0;
    int toBigListDetected = 0;

    private HashMap<String, ArrayList<Triple>> tempOPxP = new HashMap<String, ArrayList<Triple>>();
    private HashMap<String, ArrayList<Triple>> tempop_S = new HashMap<String, ArrayList<Triple>>();

    private HashMap<String, Integer> tripleToPartutPartitionMap;
    private GlobalQueryGraph queryGraph;


    static boolean partutSupport = false;

    final static boolean includeCompressed = false;

    final static boolean weighting = false;
    final static boolean includeFragments = true;
    final static boolean quad = false;
    final static String dataSetPath = "/home/keg/Desktop/BTC/btc-2009-filtered.n3";
    final static String outPutDirName = "bioportal";
    private IndexCollection indexCollection;
    private int skipToLine = 0;


    public static void main(String[] args) {

        temp2.work();
        System.exit(0);

        System.out.println("starting ..");
        Main2 o = new Main2();


        try {
            ArrayList<String> filePaths = new ArrayList<String>();
            filePaths.add("/home/keg/Desktop/BTC/n3/btc-2.n3");
            filePaths.add("/home/keg/Desktop/BTC/n3/btc-13.n3");
            o.openIndexes();
            System.out.println("loading extra index in memory..");
            o.OPxP.loadQueryTimeCahce();
            //o.printIndexesStat();
            o.listenToQuery();

        } catch (Exception e) {
            e.printStackTrace();
            o.finish();
        }


    }


    private void openIndexes() {
        ArrayList<Triple> list = new ArrayList<Triple>();
        list.add(new Triple(0, 0, 0));

        if (OPS == null)
            OPS = new MyHashMap("OPS", new IndexType(1, 1, 0));

        OPS.open(new Integer(5), list);


        if (op_S == null)
            op_S = new MyHashMap("op_S", new IndexType(1, 0, 0));

        op_S.open(" ", list);


        OPxP.setExtraIndexType(new IndexType(1, 0, 1));
        OPxP.open(" ", list);


    }



    private void clean() {
        dictionary = null;
        reverseDictionary = null;
    }

    private void setPartitionNumberInVertices(LineIterator it) {
        int count = 1;
        while (it.hasNext()) {
            String line = it.nextLine();
            int n = Integer.valueOf(line);
            verticies.get(count).partitionNumber = n;
            count++;
        }
    }

    private String buildVertixString(ArrayList<Vertex> vees, int currentVertexID, boolean weighting) {
        if (vees.size() == 0)
            return "";
        if (vees.size() > 100000)
            toBigListDetected++;//return "";
        //String res = vees.get(0).v+" "+(vees.get(0).weight1 + vees.get(0).weight2)+"";
        String res = "";
        StringBuilder stringBuilder = new StringBuilder();
        boolean first = true;
        // HashMap<Integer, Integer> map = new HashMap();
        for (int i = 0; i < vees.size(); i++) {
            int weight = 1;
            if (!weighting) {
                if (vees.get(i).e == -1) {
                    ArrayList<Vertex> other = this.graph.get(vees.get(i).v);
                    if (other == null) {
                        System.err.println("error getting other triple.Vertex list");
                        System.exit(0);
                    }


                    HashMap<Integer, Vertex> otherMap = graphMap.get(vees.get(i).v);
                    if (otherMap == null) {
                        otherMap = new HashMap();
                        for (int k = 0; k < other.size(); k++) {
                            otherMap.put(other.get(k).v, other.get(k));
                            graphMap.put(vees.get(i).v, otherMap);
                            if (other.get(k).v == currentVertexID)
                                weight = other.get(k).weight1 + other.get(k).weight2;
                        }
                    } else {
                        Vertex otherVertex = otherMap.get(currentVertexID);
                        weight = otherVertex.weight1 + otherVertex.weight2;
                    }

                } else {
                    weight = (vees.get(i).weight1 + vees.get(i).weight2);
                }
            }
            //    if(map.containsKey(vees.get(i).v) )
            //         continue;
            ///  map.put(vees.get(i).v, vees.get(i).v);
            String weightStr = null;//tempIntToStringMap.get(weight);
            if (weightStr == null) {
                weightStr = weight + "";
                tempIntToStringMap.put((int) weight, weightStr);
            }
            /*
            String destVertexID = tempIntToStringMap.get(vees.get(i).v);
            if(destVertexID == null) {
                destVertexID =  vees.get(i).v + "" ;
                tempIntToStringMap.put(vees.get(i).v, destVertexID);
            }*/
            String destVertexID = vees.get(i).v + "";
            if (first)
                // res = destVertexID + " " + weightStr + "";
                stringBuilder.append(destVertexID).append(" ").append(weightStr);
                //res = vees.get(i).v+"";
            else
                //res = res + " " + destVertexID + " " + weightStr;
                stringBuilder.append(" ").append(destVertexID).append(" ").append(weightStr);


            //res = res + " " +vees.get(i).v ;
            first = false;
            edgesWritten++;
            double progressRatioN = (double) edgesWritten / (double) this.edgesCount;
            if (progressRatioN > progressRatio + 0.005) {
                System.out.flush();

                System.out.print(String.format("\033[%dA", 1)); // Move up
                System.out.print("\033[2K"); // Erase line content

                System.out.println("writing progress:" + (100 * progressRatioN));
                progressRatio = progressRatioN;
            }
        }
        return stringBuilder.toString();
    }

    HashMap<String, String> prefix = new HashMap<String, String>();
    ArrayList<String> header;

    public void process(ArrayList<String> filePathList, boolean quad) {
        int test = 0;
        tripleToPartutPartitionMap = new HashMap();
        header = new ArrayList();
        PredicatesAsSubject = new HashMap();
        int[] code = new int[3];
        int nextCode = 1;
        int nextPredicateCode = BASE_PREDICATE_CODE;
        Integer errCount = 0;
        Integer errSolved = 0;
        int duplicateCount = 0;
        int count = 0;
        for (int k = 0; k < filePathList.size(); k++) {
            File file = new File(filePathList.get(k));
            LineIterator it = null;
            try {
                it = FileUtils.lineIterator(file, "US-ASCII");
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            try {
                while (it.hasNext() /*&& count < 10000000*/) {
                    if (count % 100000 == 0 || count == 2082) {
                        File stopFile = new File("/home/ahmed/stop");
                        if (stopFile.exists()) {
                            System.out.println("stop file detected, stopping ..");
                            finish();
                            System.exit(0);
                        }
                        if (count == 0)
                            printBuffer(0, k + " processing line " + count / 1000 + " k");
                        else
                            printBuffer(1, k + " processing line " + count / 1000 + " k");
                        checkMemory();

                    }
                    count++;
                    String line = it.nextLine();
                    if (count < skipToLine)
                        continue;
                    if (line.startsWith("@")) {
                        if (line.startsWith("@base"))
                            prefix.put("base", line.split(" ")[1]);
                        else
                            prefix.put(line.split(" ")[1].replace(":", ""), line.split(" ")[2]);
                        header.add(line);
                        continue;
                    }
                    String[] triple;
                    if (quad) {
                        triple = getTripleFromQuadLine(line);
                        if (triple == null)
                            continue;
                    } else {
                        triple = getTripleFromTripleLine(line, errCount, errSolved);


                    }
                    if (triple == null || triple.length < 3) {
                        continue;
                    }
                    if (triple[0].equals(triple[1]))
                        continue;
                    ArrayList<Vertex> v = null;
                    boolean ignoreFlag = false;
                    for (int i = 0; i < 3; i++) {
                        if (dictionary.containsKey(triple[i])) {
                            code[i] = dictionary.get(triple[i]);
                            if (i != 1 && code[i] >= BASE_PREDICATE_CODE) {
                                //ignoreFlag = true; //just ignore the predicate which came as subj or obj
                                if (!PredicatesAsSubject.containsKey(code[i])) {
                                    PredicatesAsSubject.put(code[i], nextCode);
                                    v = new ArrayList();
                                    //TODO replace the verticesID list with iteration over reverse dictinary
                                    ///                     vertecesID.add(nextCode);
                                    addToGraph(nextCode, v);
                                    //graph.put(nextCode, v);
                                    code[i] = nextCode;
                                    dictionary.put(triple[i], code[i]);
                                    reverseDictionary.put(code[i], triple[i]);
                                    nextCode++;
                                }
                            }
                            if (i == 1 && code[i] < BASE_PREDICATE_CODE) {
                                //wrong value was added fix it!
                                //dictionary.remove(triple[i]);
                                //reverseDictionary.remove(code[i]);
                                // vertecesID.remove(code[i]);
                                // graph.remove(code[i]);

                                //    code[i] = nextPredicateCode;
                                //   nextPredicateCode++;
                                //  dictionary.put(triple[i], code[i]);
                                // reverseDictionary.put(code[i], triple[i]);
                            }

                            // v = graph.get(code[i]);
                        } else {
                            if (i != 1) {
                                code[i] = nextCode;
                                nextCode++;
                                if (count >= skipToLine) {
                                    dictionary.put(triple[i], code[i]);
                                    reverseDictionary.put(code[i], triple[i]);
                                }
                                v = new ArrayList();
                                ////                  vertecesID.add(code[i]);
                                //graph.put(code[i], v);
                                addToGraph(nextCode, v);
                            } else {
                                code[i] = nextPredicateCode;
                                nextPredicateCode++;
                                if (count >= skipToLine) {
                                    dictionary.put(triple[i], code[i]);
                                    reverseDictionary.put(code[i], triple[i]);
                                }
                            }
                        }
                    }
                    if (ignoreFlag)
                        continue;
                    try {
                        /// addToVertexGraph(code);
                        if (!buildGraph(code, v)) {
                            duplicateCount++;
                            continue;
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (count < skipToLine)
                        continue;
                    if (count % 10000000 == 0) {
                        System.out.println("writing to diskDb");
                        writeTempIndex(tempop_S, op_S);
                        tempop_S = new HashMap<String, ArrayList<Triple>>();
                        if (op_S != null)
                            op_S.commitToDisk();
                    }
                    //build the tripe graph
                    Triple tripleObj = new Triple(code[0], code[1], code[2]);
               /* if (!tripleGraph.containsKey(code[0]))    seems like tripleGraph is the same as SPO, thus we dont need it
                    tripleGraph.put(code[0], new ArrayList<Triple>());
                tripleGraph.get(code[0]).add(tripleObj);*/
                    if (partutSupport) {
                        tripleToPartutPartitionMap.put(code[0] + "p" + code[1] + "p" + code[2], -1);
                    }

                    //          addToPOSIndex(tripleObj);
                    //           addToSPOIndex(tripleObj);
                    // addToOPSIndex(new Triple(88,87,43));
                    // ArrayList<Triple> OptimizerGUI = OPS.get(new Integer(2));
                    addToOPSIndex(tripleObj);
                    // if(test == 0)
                    //   test = code[2];
                    //  ArrayList<Triple> test3 = OPS.get(test);
                    //addTosp_OIndex(tripleObj);

                    addToOp_SIndex(tripleObj);
                }
            } finally {
                LineIterator.closeQuietly(it);
            }
        }
        tripleGraph = SPO;
        // op_S.close();

//        ArrayList<triple.Vertex> vv = graph.get(vertecesID.get(3));
        System.out.println("done ... errors: " + errCount + " solved:" + errSolved + ", duplicate:" + duplicateCount);
        System.out.println(" error quad processing :" + errQuadProcess + " sucess:" + quadProcess + " err start:" + startErrQuadProcess + " ratio of failure : " + (double) errQuadProcess / (double) quadProcess);

        System.out.println(" the total vetrices = " + verticies.size() + " max code = " + nextCode);
        writeTempIndex(tempop_S, op_S);
        tempop_S = new HashMap<String, ArrayList<Triple>>();
        indexCollection = new IndexCollection();
        //      indexCollection.addIndex(SPO, new IndexType(1,0,0));
        //      indexCollection.addIndex(OPS, new IndexType(1,1,0));
        //      indexCollection.addIndex(POS, new IndexType(1,1,1));
        //      indexCollection.addIndexStringKey(op_S, new IndexType(1,0,1));


    }

    private boolean buildGraph(int[] code, ArrayList<Vertex> v) {
        /*v = graph.get(code[0]);
        boolean duplicateFlag = false;
        for (int i = 0; i < v.size(); i++)
            if (v.get(i).v == code[2]) {
                duplicateFlag = true;
                break;
            }
        if (duplicateFlag)
            return false;
        v.add(new Vertex(code[2], code[1]));
        edgesCount++;
        v = graph.get(code[2]);
        v.add(new Vertex(code[0], -1));*/
        return true;
    }

    private void addToGraph(int code, ArrayList<Vertex> v) {
        //graph.put(code, v);
    }





    static ArrayList<String> printBuffer;

    static public void printBuffer(int countToDelete, String s) {
        if (printBuffer == null)
            printBuffer = new ArrayList();
        for (int i = 0; i < countToDelete; i++) {
            if (printBuffer.size() > 0)
                printBuffer.remove(printBuffer.size() - 1);
            else
                break;
        }
        printBuffer.add(s);
        for (int i = 0; i < 50; i++) {
            System.out.println();
        }
       /* try {
            Runtime.getRuntime().exec("clear");
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        for (int i = 0; i < printBuffer.size(); i++) {
            System.out.println(printBuffer.get(i));
        }

    }


    private void writeDictionary() {
        System.out.println("writing dictionary to temp file");
        BufferedWriter bw = null;
        FileWriter fw = null;
        Iterator it = dictionary.entrySet().iterator();
        try {
            fw = new FileWriter("/home/ahmed/download/dictionary_temp.n3");
            bw = new BufferedWriter(fw);
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                String line = pair.getKey() + " powerx1983s " + pair.getValue();
                bw.write(line);
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bw.close();
                fw.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        System.out.println("done");
    }

    private void readDictioanry() {

        System.out.println("reading dictionary from temp file");
        dictionary = new Dictionary("dictionary");
        //    reverseDictionary = new HashMap();
        File file = new File("/home/ahmed/download/dictionary_temp.n3");
        LineIterator it = null;
        try {
            it = FileUtils.lineIterator(file);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        while (it.hasNext()) {
            String line = it.nextLine();
            String[] strArr = line.split(" powerx1983s ");
            Integer key = Integer.valueOf(strArr[1]);
            dictionary.put(strArr[0], key);
            //  reverseDictionary.put(key, strArr[0]);
        }
        System.out.println("done");
    }


    private String[] getTripleFromTripleLine(String line, Integer errCount, Integer errSolved) {
        String[] triple = new String[3];
        String[] linet = new String[1];
        linet[0] = line;
        triple[0] = stripItem(linet);
        triple[1] = stripItem(linet);
        triple[2] = stripItem(linet);
        if (triple[2] != null)
            triple[2] = triple[2];
        if (triple[0] != null && triple[1] != null && triple[2] != null)
            return triple;
        errCount++;
        return null;
    }

    private String[] getTripleFromTripleLine_old(String line, Integer errCount, Integer errSolved) {
        String[] triple = new String[3];
        triple = line.split(" ");
        if (triple.length > 4) {
            errCount++;
            try {
                triple[0] = line.substring(line.indexOf("<"), line.indexOf(">") + 1);
                if (line.length() - line.replace(triple[0], "").length() > triple[0].length() + 2)
                    line = line.replaceFirst(triple[0], "");
                else
                    line = line.replace(triple[0], "");

            } catch (PatternSyntaxException e) {
                e.printStackTrace();
                return null;
            }
            try {
                int from, to;
                if (line.contains("\"")) {
                    from = line.indexOf('\"');
                    to = line.lastIndexOf('.');
                    triple[2] = line.substring(from, to + 1);
                } else {
                    from = line.lastIndexOf("<");
                    to = line.lastIndexOf(">") + 1;
                    triple[2] = line.substring(from, to);
                }
                /*
                if (line.contains("<") && !line.trim().startsWith("<")) {
                    from = line.lastIndexOf("<");
                    to = line.lastIndexOf(">") + 1;
                    triple[2] = line.substring(from, to);
                } else {
                    from = line.indexOf('\"');
                    to = line.lastIndexOf('.');
                    triple[2] = line.substring(from, to + 1);
                }*/

                //triple[1] = line.replaceFirst(triple[2], "");
                triple[1] = line.substring(0, from);
                triple[1] = triple[1].trim();
            } catch (StringIndexOutOfBoundsException e) {
                e.printStackTrace();
                System.out.println(triple[0] + "   " + triple[1] + "     " + triple[2]);
                return null;
            }
            //continue;
            errSolved++;
        }
        String[] arr = triple[2].split(">");
        //arr[1] = arr[1].replace(".","");
        triple[2] = arr[0] + ">";
        return triple;

    }


    private String stripItem(String[] linet) {
        String line = linet[0].trim();
        String item = null;
        char startChar = line.charAt(0);
        switch (startChar) {
            case '\"':
                line = line.substring(1);
                for (int i = 1; i < line.length(); i++) {
                    char c = line.charAt(i);
                    if (c == '\"' && line.charAt(i - 1) != '\\') {
                        item = line.substring(0, i + 1);
                        line = line.substring(i + 1);
                        linet[0] = line;
                        return "\"" + item;
                    }
                }
                if (line.startsWith("\""))
                    return "\"" + "\"";
                return null;

            case '<':
                item = line.substring(0, line.indexOf('>') + 1);
                line = line.substring(line.indexOf('>') + 1);
                linet[0] = line;
                return item;
            case '_':
                item = line.substring(0, line.indexOf(' ') + 1);
                line = line.substring(line.indexOf(' ') + 1);
                linet[0] = line;
                return item;
        }
        return null;
    }

    private int errQuadProcess = 0;
    private int startErrQuadProcess = 0;
    private int quadProcess = 0;

    private String[] getTripleFromQuadLine(String line) {
        if (line.contains("<http://purl.org/rss/1.0/modules/content/encoded>"))
            line.contains("");
        String t = line.replace("qqq", "");
        if (!line.startsWith("<") && !line.startsWith("_:")) {
            errQuadProcess++;
            startErrQuadProcess++;
            return null;
        }
        String[] triple = new String[3];
        String[] linet = new String[1];
        linet[0] = line;
        triple[0] = stripItem(linet);
        triple[1] = stripItem(linet);
        triple[2] = stripItem(linet);
        if (triple[2] != null)
            triple[2] = triple[2] + '.';
        if (triple[0] != null && triple[1] != null && triple[2] != null)
            return triple;
        errQuadProcess++;
        return null;
    }


    private void addToOp_SIndex(Triple triple) {
        if (op_S == null)
            op_S = new MyHashMap("op_S", new IndexType(1, 0, 0), false);
        int code[] = triple.triples;
        String key = code[2] + "" + code[1];
        op_S.addTripleLazy(key, triple);
        //addToIndex(op_S, triple, key);
        //addToIndexTemp(tempop_S,triple,key);
    }


    private void writeTempIndex(HashMap temp, MyHashMap persist) {
        Iterator it = temp.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            String key = (String) pair.getKey();
            ArrayList val = (ArrayList) pair.getValue();
            persist.put(key, val);
        }
    }

    private void addToIndex(MyHashMap<String, ArrayList<Triple>> index, Triple triple, String key) {
        if (index.containsKey(key)) {
            index.appendToTripleList(key, triple);
        } else {
            ArrayList<Triple> list = new ArrayList();
            list.add(triple);
            index.put(key, list);
        }
    }


    private void addToPOSIndex(Triple triple) {
        if (POS == null)
            POS = new HashMap();
        //triple.triple triple = new triple.triple(code[0], code[1], code[2]);
        if (POS.containsKey(triple.triples[1])) {
            POS.get(triple.triples[1]).add(triple);
        } else {
            ArrayList<Triple> list = new ArrayList();
            list.add(triple);
            POS.put(triple.triples[1], list);
        }
    }

    private void addToSPOIndex(Triple triple) {
        int code[] = triple.triples;
        if (SPO == null)
            SPO = new MyHashMap("SPO");
        if (SPO.containsKey(code[0])) {
            SPO.get(code[0]).add(triple);
        } else {
            ArrayList<Triple> list = new ArrayList();
            list.add(triple);
            SPO.put(code[0], list);
        }
    }

    private void addToOPSIndex(Triple triple) {
        int code[] = triple.triples;
        if (OPS == null)
            OPS = new MyHashMap("OPS", new IndexType(1, 1, 0));
        OPS.addTripleLazy(code[2], triple);
        /*
        if (OPS.containsKey(code[2])) {
            OPS.get(code[2]).add(triple);
        } else {
            ArrayList<Triple> list = new ArrayList();
            list.add(triple);
            OPS.put(code[2], list);
        }*/
    }


    private ArrayList<Query> generateQueries() {
        QueryGenrator queryGenrator = new QueryGenrator(5, 4, verticies, 4, tripleGraph, 40);
        //ArrayList<QueryStuff.Query> queries = queryGenrator.buildQueries(10);
        ArrayList<Query> queries = queryGenrator.buildHeavyQueries(10);
        return queries;
    }


    private void writePalinQueriesToFile(ArrayList<String> queries) {
        BufferedWriter bw = null;
        FileWriter fw = null;
        try {
            fw = new FileWriter("queriesFile");
            bw = new BufferedWriter(fw);
            for (int i = 0; i < queries.size(); i++) {
                bw.write(queries.get(i));
                bw.newLine();
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null)
                    bw.close();
                if (fw != null)
                    fw.close();
            } catch (IOException ex) {

                ex.printStackTrace();

            }
        }
    }


    private int oppsCurrentCount = 0;

    private void addToOppIndex(Integer O, ArrayList<Triple> list1) {
        for (int i = 0; i < list1.size(); i++) {
            Triple triple1 = list1.get(i);
            int P1 = triple1.triples[1];
            int s = triple1.triples[0];
            //check each s as o in the OPS
            ArrayList<Triple> list2 = OPS.get(s);
            if (list2 == null)
                continue;
            for (int j = 0; j < list2.size(); j++) {
                Triple Triple = list2.get(j);
                int P2 = Triple.triples[1];
                String key = O + "" + P1 + "" + P2;
                addToQueryCandidate(key);
                OPxP.addTripleLazy(key, triple1);
                OPxP.addTripleLazy(key, Triple);
                // addToIndexTemp(OPxP, triple1, key);
                //addToIndexTemp(OPxP, Triple, key);
                //addToIndex(OPxP, triple1, key);
                // addToIndex(OPxP, Triple, key);

                if (oppsCurrentCount % 100000 == 0) {
                    for (int m = 0; m < 50; m++)
                        System.out.println();
                    System.out.flush();
                    System.out.println("adding triple " + oppsCurrentCount / 1000 + " k, writting buffer size = " + OPxP.getWritingThreadBufferSize() / 1000 + " k");
                    File stopFile = new File("/home/ahmed/stop");
                    if (stopFile.exists()) {
                        System.out.println("stop file detected, stopping ..");
                        finish();
                        System.exit(0);
                    }
                }
                oppsCurrentCount++;

            }

        }
    }


    private ArrayList<String> queryKeys = new ArrayList<String>();

    private void addToQueryCandidate(String key) {
        if (queryKeys.size() > 40)
            return;
        int ch = new Random().nextInt(100) + 1;
        if (ch == 10)
            queryKeys.add(key);

    }

    private void generateQueryGraph(ArrayList<Query> queries) {
        queryGraph = new GlobalQueryGraph(queries, 4, POS, SPO, OPS, verticies);
        queryGraph.genrate();


    }


    public void convertNqToN3(String sourceFile, String destFile, int from, int to, int mult) {
        String path;
        for (int i = from; i <= to; i++) {
            path = sourceFile + (i * mult);
            convertNqToN3(path, destFile, true);
        }
    }


    public void convertNqToN3(String sourceFile, String destFile, boolean append) {
        boolean quad = true;
        int err = 0;
        int intTextErr = 0;
        int linesWrittenCount = 0;
        File file = new File(sourceFile);
        LineIterator it = null;
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            it = FileUtils.lineIterator(file, "US-ASCII");
            fw = new FileWriter(destFile, append);
            bw = new BufferedWriter(fw);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        int duplicateCount = 0;
        int count = 0;
        while (it.hasNext()) {
            try {
                String lineTemp;
                if (count % 1000000 == 0) {
                    System.out.println("processing line " + count);
                }
                count++;
                if (count == 495)
                    count += 0;
                String line = it.nextLine();
                String[] triple = null;


                if (quad) {
                    triple = getTripleFromQuadLine(line);
                    if (triple == null) {
                        err++;
                        continue;
                    }
                }

                if (triple == null || triple.length < 3)
                    continue;

                //write the triple to bufferfile
                String tripleStr = triple[0] + " " + triple[1] + " " + triple[2];
                if (triple[0].length() > 150) {
                    intTextErr++;
                    continue;
                }
                // if (linesWrittenCount > 1220 ){
                bw.write(tripleStr);
                bw.newLine();
                // }
                linesWrittenCount++;

            } catch (Exception e) {
                e.printStackTrace();
            }

        }


        try {
            if (bw != null)
                bw.close();
            if (fw != null)
                fw.close();
        } catch (IOException ex) {

            ex.printStackTrace();

        }
        System.out.println("convertion Done, writen" + linesWrittenCount + " .. errors:" + err + " ,int text error:" + intTextErr);

    }

    private boolean ignoreCovertedLine(int count) {
        int[] arr = {543466, 1347540, 1347541, 1347548, 1347551, 3845831, 4102660, 4368091, 6007830};
        for (int i = 0; i < arr.length; i++) {
            if (count == arr[i] - 1 || count == arr[i])
                return true;
        }
        return false;
    }


    private void listenToQuery() {
        /*
        int a = 2;
        dictionary = new HashMap();
            dictionary.put("<http://mpii.de/yago/resource/describes>",a++);
            dictionary.put("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",a++);
            dictionary.put("<http://www.w3.org/2000/01/rdf-schema#subClassOf>",a++);
            dictionary.put("<http://mpii.de/yago/resource/isPartOf>",a++);
            dictionary.put("<http://mpii.de/yago/resource/wordnet_transportation_system_104473432>",a++);*/


        Scanner scanner = new Scanner(System.in);
        while (true) {
            try {
                System.out.println("Please enter Sparql QueryStuff.Query:");
                String query = scanner.nextLine();
                if (query.matches("e")) {
                    System.out.println("Exiting query system..");
                    finish();
                    System.out.println("done ..");
                    return;
                }
                if (query.startsWith("g")) {
                    String s = query.replace("g", "").trim();
                    double memPercent = 0;
                    if (!s.equals(""))
                        memPercent = Double.valueOf(s);
                    if (memPercent > 1)
                        memPercent = memPercent / 100;

                    ArrayList<String> HeaveyQueries = QueryGenrator.buildFastHeavyQuery(OPxP, OPS, vertecesID.size(), reverseDictionary, queryKeys, memPercent);
                    writePalinQueriesToFile(HeaveyQueries);
                    System.out.println("done ..queries written to file..");
                    continue;
                }
                StringBuilder extTime = new StringBuilder();
                if (Query.sep()) {
                    Query spQuery_t = new Query(dictionary, query, new IndexesPool(null, null), null, null);
                    spQuery_t.findChainQueryAnswer(OPxP, op_S, extTime);
                    extTime = new StringBuilder();
                }
                long startTime = System.nanoTime();
                Query spQuery = new Query(dictionary, query, new IndexesPool(null, null), null, null);
                long parseTime = System.nanoTime();
                spQuery.findChainQueryAnswer(OPxP, op_S, extTime);
                long stopTime = System.nanoTime();
                long elapsedTime = (stopTime - startTime) / 1000;
                spQuery.printAnswers(reverseDictionary);
                System.out.println("time to execute qeury:" + elapsedTime + " micro seconds," + " time to OPxP " + extTime + " Ms, parse time:" + (parseTime - startTime) / 1000 + " Ms");
            } catch (Exception e) {
                System.err.println("unable to parse query..");
                //  e.printStackTrace();
            }
        }
    }

    private void finish() {
        if (OPS != null)
            OPS.close();
        if (OPxP != null)
            OPxP.close();
        if (op_S != null)
            op_S.close();
        if (dictionary != null)
            dictionary.close();
    }


    private void checkMemory() {
        long rem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long max = Runtime.getRuntime().maxMemory();
        System.out.println(" app used memory: " + rem / 1000000000 + " GB");
        if ((max - rem) / 1000 < 2000000) {
            System.out.println(" Low memory detected, exiting ... ");
            finish();
            System.exit(1);
        }
    }


}
