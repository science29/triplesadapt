package start;

import QueryStuff.*;
import distiributed.Transporter;
import index.Dictionary;
import index.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import triple.Triple;
import triple.Vertex;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.regex.PatternSyntaxException;


public class MainUinAdapt {

    private static final int BASE_PREDICATE_CODE = 1000000000;
    private static Transporter transporter;
    private VertexGraphIndex graph = new VertexGraphIndex("graph");

    private HashMap<Integer, ArrayList<Triple>> tripleGraph = new HashMap();//duplicate with graph , remove one!
    private ArrayList<Integer> vertecesID = new ArrayList();
    private HashMap<Integer, VertexGraph> verticies = new HashMap<Integer, VertexGraph>();;
    private Dictionary dictionary = new Dictionary("dictionary_int");
    //private HashMap<Integer, String> reverseDictionary = new HashMap();
    Dictionary reverseDictionary = dictionary;
    private HashMap<Integer, ArrayList<VertexGraph>> distanceVertex;
    private HashMap<Integer, Integer> PredicatesAsSubject;
    private int edgesCount = 0;
    private static final int PARTITION_COUNT = 2;

    private MyHashMap<Integer, ArrayList<Triple>> POS;
    private MyHashMap<Integer, ArrayList<Triple>> SPO;
    private MyHashMap<Integer, ArrayList<Triple>> OPS;
    private MyHashMap<String, ArrayList<Triple>> sp_O;
    private MyHashMap<String, ArrayList<Triple>> op_S;
    private MyHashMap<String, ArrayList<Triple>> SPxP = new MyHashMap("SPxP");
    private MyHashMap<String, ArrayList<Triple>> OPxP = new MyHashMap("OPxP" , new IndexType(1,0,0));


    private HashMap<String , ArrayList<Triple>> tempOPxP = new HashMap<String, ArrayList<Triple>>();
    private HashMap<String , ArrayList<Triple>> tempop_S = new HashMap<String, ArrayList<Triple>>();

    private HashMap<String, Integer> tripleToPartutPartitionMap;
    private GlobalQueryGraph queryGraph;


    static boolean partutSupport = false;

    final static boolean includeCompressed = false;

    final static boolean weighting = false;
    final static boolean includeFragments = true;
    final static boolean quad = false;
    //final static String dataSetPath = "/home/ahmed/download/btc-data-3.nq";
    final static String dataSetPath = "/home/keg/Desktop/BTC/btc-2009-filtered.n3";// "/home/keg/Desktop/BTC/btc_small.n3";   ////"../RDFtoMetis/btc-2009-small.n3";//"/afs/informatik.uni-goettingen.de/user/a/aalghez/Desktop/RDF3X netbean/rdf3x-0.3.7/bin/yago_utf.n3";
    // final static String dataSetPath = "/home/keg/Downloads/rexo.nq";
    final static String outPutDirName = "bioportal";
    private IndexCollection indexCollection;
    private int skipToLine = 0;
    private IndexesPool indexPool;
    private HashMap<Integer,Boolean> borderTripleMap = new HashMap<>();

    private QueryWorkersPool queryWorkersPool;


    public static void main(String[] args) {


        System.out.println();
        System.out.println("starting ..");

        MainUinAdapt o = new MainUinAdapt();

try {
    ArrayList<String> filePaths = new ArrayList<String>();
    filePaths.add("/home/keg/Desktop/BTC/yago.n3");
    o.openIndexes();
    System.out.println("starting transporter ..");
    ArrayList<String> hosts = new ArrayList<>();
    hosts.add("172.20.32.8");
    hosts.add("172.20.32.7");

    transporter = new Transporter(hosts, new Transporter.RemoteQueryListener() {
        @Override
        public void gotQuery(String query, int queryNo) {
            if(o.queryWorkersPool != null)
                o.queryWorkersPool.addQuery(query , queryNo);

        }
        @Override
        public void queryDone(int queryNo) {
            o.queryWorkersPool.queryDone(queryNo);
        }
    });

    try {
     ////   o.porcess(filePaths, quad);
    }catch (Exception e){
        e.printStackTrace();
    }
    transporter.printSummary();
    o.listenToQuery();

}catch (Exception e){
    e.printStackTrace();
    o.finish();
}

       /*  ArrayList<QueryStuff.Query> queries = o.generateQueries();

        System.out.println("Writting queries to file");
        o.putStringTripleInQueries(queries);
        o.writeQueriesToFile(queries);

       System.exit(0);





        // System.out.println("setting dynamic weigths .. ");
        o.setDynamicWeights(queries);


        System.out.println("writting the 1st metis input file");

      ///  o.writeFile(weighting);
        System.out.println(" skip writing .. ");

        System.out.println("please do the 1st metis partitioining then press any key to continue");
        Scanner scanner = new Scanner(System.in);
        scanner.next();

        o.writeDictionary();
        o.clean();
        o.setPartionNumberFromMetis("metisFile_new.part." + PARTITION_COUNT);
        o.setDistVervtices(4);
        System.out.println("Generating queries and fragments .. ");
        o.generateQueryGraph(queries);

        o.writeFile(weighting);
        o.readDictioanry();
        System.out.println("please do the 2nd metis partitioining then press any key to continue");
        scanner = new Scanner(System.in);
        scanner.next();
        if (!partutSupport)
            o.readOutput("metisFile_new.part." + PARTITION_COUNT, PARTITION_COUNT, includeFragments);
        else
            o.writePartutPartitions(PARTITION_COUNT);

*/

    }


    private void openIndexes() {
        ArrayList<Triple> list = new ArrayList<Triple>();
        list.add(new Triple(0,0,0));

        if (OPS == null)
            OPS = new MyHashMap("OPS" , new IndexType(1,1,0) );

        OPS.open(new Integer(5),list);


          if (op_S == null)
            op_S = new MyHashMap("op_S" , new IndexType(1,0,0));

        op_S.open(" ",list);


        OPxP.setExtraIndexType(new IndexType(1,0,1));
        OPxP.open(" ",list);


    }

    private void setDynamicWeights(ArrayList<Query> queries) {
        GlobalQueryGraph.setDynamicWeights(queries, tripleGraph, graph, POS);
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

    private boolean setPartionNumberFlag = false;

    private void setPartionNumberFromMetis(String filePath) {
        File file = new File(filePath);
        LineIterator it = null;
        try {
            it = FileUtils.lineIterator(file, "UTF-8");
            //first set the partitions into vertices
            setPartitionNumberInVertices(it);
            setPartionNumberFlag = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private HashMap<Integer, VertexGraph> toBeCheckedVertexes; // this is the list of non boraders vertex that need to be checked if they have been written before the border ones

    private void readOutput(String filePath, int partitionCount, boolean includeFragments) {
        toBeCheckedVertexes = new HashMap();
//read the meits output and our graph then rewrite the rdf files
        BufferedWriter[] outBuff = new BufferedWriter[partitionCount];
        FileWriter[] outfw = new FileWriter[partitionCount];

        ArrayList<String>[] out_ext_list = new ArrayList[partitionCount];
        ArrayList<String>[] compressed_ext_list = new ArrayList[partitionCount];
        // FileWriter []  outfw_ext = new FileWriter[partitionCount];
        try {
            for (int i = 0; i < partitionCount; i++) {
                outfw[i] = new FileWriter("/home/ahmed/rdfToMetisoutput/out/" + outPutDirName + i + ".n3");
                outBuff[i] = new BufferedWriter(outfw[i]);
                out_ext_list[i] = new ArrayList();
                compressed_ext_list[i] = new ArrayList();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        File file = new File(filePath);
        LineIterator it = null;
        try {
            it = FileUtils.lineIterator(file, "UTF-8");
            //first set the partitions into vertices, if it is nont already done
            if (!setPartionNumberFlag) {
                setPartitionNumberInVertices(it);
                setPartionNumberFlag = true;
            }
            //reset the ierator
            it = FileUtils.lineIterator(file, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        //setting the border vertices ..
        setBorderVertices();

        //temp code please remvoe


        //end temp code
        try {
            //write the header:
            for (int r = 0; r < partitionCount; r++)
                for (int s = 0; s < header.size(); s++) {
                    outBuff[r].write(header.get(s) + "");
                    outBuff[r].newLine();
                }

            int count = 1;
            while (it.hasNext()) {
                String line = it.nextLine();
                int n = Integer.valueOf(line);
                ArrayList<Vertex> v = graph.get(count);
                String triple = "";
                boolean write = false;
                for (int i = 0; i < v.size(); i++) {
                    if (v.get(i).e < 0)
                        continue;
                    triple = reverseDictionary.get(count) + " " + reverseDictionary.get(v.get(i).e) + " " + reverseDictionary.get(v.get(i).v);

                    if (!triple.endsWith(".")) {
                        int p1 = verticies.get(v.get(i).v).partitionNumber;
                        int p2 = verticies.get(count).partitionNumber;

                        /*if(p1 == -1 || p2 == -1)
                            break;
                        if (p1 != p2)*/
                        boolean bsource = false;//isBorder(count);
                        boolean bdest = v.get(i).isBorder;//isBorder(v.get(i).v);
                        if (bsource || bdest) {
                            if (!bsource && !verticies.get(count).writtenToFile) // it is not border and not been written yet
                                toBeCheckedVertexes.put(count, verticies.get(count)); // add to chekmap
                            if (!bdest && !verticies.get(v.get(i).v).writtenToFile) // it is not border and not been written yet
                                toBeCheckedVertexes.put(count, verticies.get(v.get(i).v)); // add to chekmap
                            triple = triple + " .";
                            out_ext_list[n].add(triple);
                            compressed_ext_list[n].add(triple);
                            boolean added = verticies.get(v.get(i).v).addLinkInPartition(p2);
                           /* if (added && reverseDictionary.get(v.get(i).v).contains("<") && reverseDictionary.get(v.get(i).v).contains(">")) {
                                String InBorderTriple = reverseDictionary.get(v.get(i).v) + " " + "x:isRefferedBy" + " " + "\"" + p2 + "\"";
                                outBuff[p1].write(InBorderTriple + " .");
                                outBuff[p1].newLine();
                            }*/
                        } else {
                            triple = triple + " .";
                            outBuff[n].write(triple);
                            compressed_ext_list[n].add(triple);
                            write = true;
                            outBuff[n].newLine();
                            verticies.get(v.get(i).v).writtenToFile = true;
                            verticies.get(count).writtenToFile = true;
                            verticies.get(count).writenToPartitionsFilesMap.put(n, 0);
                            verticies.get(v.get(i).v).writenToPartitionsFilesMap.put(n, 2);
                        }
                        // verticies.get(count).partitionNumber = n ;

                    }

                }

                count++;
                if (count % 100000 == 0)
                    System.out.println(count + " : " + triple);
            }
            //do final extar writing to toBeCheckedVertexes
            Iterator itmap = toBeCheckedVertexes.entrySet().iterator();
            int dummyCount = 0;
            while (itmap.hasNext()) {
                Map.Entry pair = (Map.Entry) itmap.next();
                VertexGraph vx = (VertexGraph) pair.getValue();
                if (vx.writtenToFile)
                    continue;
                if (reverseDictionary.get(vx.ID).contains("<") && reverseDictionary.get(vx.ID).contains(">")) {
                    dummyCount++;
                    String dummyTriple = reverseDictionary.get(vx.ID) + " " + "x:dummyRef" + " " + "\"" + 11 + "\""; // just write a dummy tripple to make this (non border triple) appears in the data set first before the writing the borders ones
                    outBuff[vx.partitionNumber].write(dummyTriple + " .");
                    outBuff[vx.partitionNumber].newLine();
                    vx.writtenToFile = true;
                    vx.writenToPartitionsFilesMap.put(vx.partitionNumber, -1);
                }
            }
            System.err.println("writing " + dummyCount + " dummy tripples");
            toBeCheckedVertexes = null;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            LineIterator.closeQuietly(it);
            try {
                //first write all the remaining triples at the end
                for (int j = 0; j < partitionCount; j++) {
                    int g = 2;
                    String ss = reverseDictionary.get(g);
                    String endTriple = ss + " " + "x:isOwnedBy" + " \"" + "ahmed_alghezi" + "\"";
                    outBuff[j].write(endTriple + " .");
                    outBuff[j].newLine();
                    for (int k = 0; k < out_ext_list[j].size(); k++) {
                        String line = out_ext_list[j].get(k);
                        outBuff[j].write(line);
                        outBuff[j].newLine();
                    }
                    if (includeFragments) {
                        String repTriple = ss + " " + "x:isOwnedBy" + " \"" + "ahmed_alghezi2" + "\"";
                        outBuff[j].write(repTriple + " .");
                        outBuff[j].newLine();
                    }
                }

                HashMap<String, Integer> writtenTripleFragment = new HashMap();
                //write the fragments if required:
                if (includeFragments) {
                    if (queryGraph == null) {
                        System.err.println("no global query graph found, consider buliding the query graph first");
                        System.exit(1);
                    }
                    ArrayList<AnnomizedTriple> fragmentsTriple = queryGraph.getAnnomizedTriples();
                    int nonBorderErr = 0;
                    int total = 0;
                    for (int i = 0; i < fragmentsTriple.size(); i++) {
                        AnnomizedTriple anmoizedTriple = fragmentsTriple.get(i);
                        ArrayList<Triple> triples = anmoizedTriple.fragment.triples;
                        for (int j = 0; j < triples.size(); j++) {
                            total++;
                            VertexGraph vs = verticies.get(triples.get(j).triples[0]);
                            int sourcePartitionNr = vs.partitionNumber;
                            VertexGraph vd = verticies.get(triples.get(j).triples[2]);

                            if (sourcePartitionNr < 0 || sourcePartitionNr >= partitionCount) {
                                System.err.println("(Main255) error in partition number:" + sourcePartitionNr);
                                System.exit(1);
                            }
                            for (int k = 0; k < partitionCount; k++) {//add the triple of fragment to all partitions excepts the owner one
                                if (k == sourcePartitionNr)//skipe the owner partition
                                    continue;
                                if (vd.writenToPartitionsFilesMap.containsKey(k)) {
                                    nonBorderErr++;
                                }
                                String tripleLine = reverseDictionary.get(triples.get(j).triples[0]) + " " + reverseDictionary.get(triples.get(j).triples[1]) + " " + reverseDictionary.get(triples.get(j).triples[2]);
                                outBuff[k].write(tripleLine + " .");
                                outBuff[k].newLine();
                                writtenTripleFragment.put(tripleLine, triples.get(j).triples[1]);
                            }
                        }

                    }
                    if (nonBorderErr > 0)
                        System.err.println("not border in fragment since it is already written to file !" + nonBorderErr + " from : " + total);
                }

                //now include the compressed replication

                if (includeCompressed) {
                    for (int k = 0; k < partitionCount; k++) {//add the triple of fragment to all partitions excepts the owner one
                        int g = 2;
                        String ss = reverseDictionary.get(g);
                        String endTriple = ss + " " + "x:isOwnedBy" + " \"" + "compressed_tripless" + "\"";
                        outBuff[k].write(endTriple + " .");
                        outBuff[k].newLine();
                        for (int m = 0; m < partitionCount; m++) {
                            if (k == m)
                                continue;
                            for (int r = 0; r < compressed_ext_list[m].size(); r++) {
                                String line = compressed_ext_list[m].get(r);
                                if (writtenTripleFragment.containsKey(line))
                                    continue;
                                outBuff[k].write(line);
                                outBuff[k].newLine();
                            }
                        }
                    }
                }


                for (int j = 0; j < partitionCount; j++) {
                    if (outBuff[j] != null)
                        outBuff[j].close();
                    if (outfw[j] != null)
                        outfw[j].close();
                }
            } catch (IOException ex) {

                ex.printStackTrace();

            }
        }
        System.out.println("done reading input ... ");

    }

    private boolean isBorder(int vid) {
        int sp = verticies.get(vid).partitionNumber;
        ArrayList<Vertex> v = graph.get(vid);
        for (int i = 0; i < v.size(); i++) {
            int cp = verticies.get(v.get(i).v).partitionNumber;
            if (sp != cp)
                return true;
        }
        return false;
    }

    private void setBorderVertices() {
        System.out.println("setting the border vertices .. ");
        for (int i = 0; i < vertecesID.size(); i++) {
            int sp = verticies.get(vertecesID.get(i)).partitionNumber;
            ArrayList<Vertex> ves = graph.get(vertecesID.get(i));
            for (int j = 0; j < ves.size(); j++) {
                int cp = verticies.get(ves.get(j).v).partitionNumber;
                if (sp != cp)
                    ves.get(j).isBorder = true;
            }
        }
        System.out.println("done ");
    }


    private void setDistVervtices(int maxDist) {
        distanceVertex = new HashMap();
        distanceVertex.put(0, new ArrayList());
        //set distane zero
        for (int i = 0; i < vertecesID.size(); i++) {
            VertexGraph vertexGraph = verticies.get(vertecesID.get(i));
            for (int j = 0; j < vertexGraph.edgesVertex.size(); j++) {
                int toVertexID = vertexGraph.edgesVertex.get(j);
                VertexGraph toVertex = verticies.get(toVertexID);
                if (toVertex.partitionNumber == -1) {
                    System.err.println("error setting the distance ..the partition number is not set ... please check if the partitions number are set ..");
                    System.exit(1);
                }
                if (toVertex.partitionNumber != vertexGraph.partitionNumber) {
                    if (toVertex.dist != 0) {
                        toVertex.dist = 0;
                        distanceVertex.get(0).add(toVertex);
                    }
                    if (vertexGraph.dist != 0) {
                        vertexGraph.dist = 0;
                        distanceVertex.get(0).add(vertexGraph);
                    }
                }

            }
        }
        System.out.println("setting 0 dist : " + distanceVertex.get(0).size() + " vertecies");
        for (int i = 1; i < maxDist; i++) {
            ArrayList<VertexGraph> newDistList = new ArrayList();
            distanceVertex.put(i, newDistList);
            ArrayList<VertexGraph> prevVertices = distanceVertex.get(i - 1);
            for (int j = 0; j < prevVertices.size(); j++) {
                VertexGraph edgeVert = prevVertices.get(j);
                for (int k = 0; k < edgeVert.edgesVertex.size(); k++) {
                    int vid = edgeVert.edgesVertex.get(k);
                    VertexGraph v = verticies.get(vid);
                    if (v.dist == -1) {
                        v.dist = edgeVert.dist + 1;
                        newDistList.add(v);
                    }
                }
            }
            System.out.println("setting " + i + " dist : " + newDistList.size() + " vertecies");
        }

    }


    private ArrayList<Integer> findSubjectVertexList() {
        ArrayList<Integer> res = new ArrayList();
        for (int i = 0; i < vertecesID.size(); i++) {
            ArrayList<Vertex> ves = graph.get(vertecesID.get(i));
            for (int j = 0; j < ves.size(); j++)
                if (ves.get(j).e != -1) {
                    res.add(vertecesID.get(i));
                    break;
                }
        }
        return res;

    }

    private void writeFile(boolean weighting) {

        BufferedWriter bw = null;
        FileWriter fw = null;
        int linesWrittenCount = 0;
        // ArrayList s_list = findSubjectVertexList();
        try {
            fw = new FileWriter("metisFile_new");
            bw = new BufferedWriter(fw);
            bw.write(vertecesID.size() + " " + edgesCount + " 001");
            bw.newLine();
            for (int i = 0; i < vertecesID.size(); i++) {
                ArrayList<Vertex> ves = graph.get(vertecesID.get(i));
                String str = buildVertixString(ves, vertecesID.get(i), weighting);
      /*      if (str.matches("")){
                System.err.println("error: empty line detected");
                System.exit(1);
            }*/

                linesWrittenCount++;
                bw.write(str);
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
        if (linesWrittenCount != vertecesID.size() || edgesWritten != 2 * edgesCount) {
            System.err.println("error: not all edges or vertices were written to file");
            //System.exit(2);
        }
        System.err.println(toBigListDetected + " big vertices list was detected.");
        System.out.println("done writing...");
    }

    private int edgesWritten = 0;
    private HashMap<Integer, HashMap<Integer, Vertex>> graphMap = new HashMap();
    private HashMap<Integer, String> tempIntToStringMap = new HashMap();
    double progressRatio = 0;
    int toBigListDetected = 0;

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

    public void porcess(ArrayList<String> filePathList, boolean quad) {
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
        for(int k =0 ; k < filePathList.size() ; k++) {
            File file = new File(filePathList.get(k));
            LineIterator it = null;
            try {
                it = FileUtils.lineIterator(file, "US-ASCII");
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            try {
                while (it.hasNext() && count < 10000000) {
                    if (count % 100000 == 0 || count == 2082) {
                        File stopFile = new File("/home/ahmed/stop");
                        if(stopFile.exists()){
                            System.out.println("stop file detected, stopping ..");
                            finish();
                            System.exit(0);
                        }
                        if (count == 0)
                            printBuffer(0, k+" processing line " + count / 1000 + " k");
                        else
                            printBuffer(1, k+" processing line " + count / 1000 + " k");
                        checkMemory();

                    }
                    count++;
                    String line = it.nextLine();
                    if(count < skipToLine)
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
                                if(count>=skipToLine ) {
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
                                if(count>=skipToLine ) {
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
                    if(count < skipToLine )
                        continue;
                    if (count % 10000000 == 0) {
                        System.out.println("writing to diskDb");
                        writeTempIndex(tempop_S,op_S);
                        tempop_S = new HashMap<String, ArrayList<Triple>>();
                        if(op_S != null)
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

                    addToPOSIndex(tripleObj);
                    addToOPSIndex(tripleObj);
                    addToSPOIndex(tripleObj);

                    //           addToSPOIndex(tripleObj);
                   // addToOPSIndex(new Triple(88,87,43));
                   // ArrayList<Triple> test2 = OPS.get(new Integer(2));

                   // if(test == 0)
                     //   test = code[2];
                  //  ArrayList<Triple> test3 = OPS.get(test);
                    //addTosp_OIndex(tripleObj);

                   // addToOp_SIndex(tripleObj);
                }
            } finally {
                LineIterator.closeQuietly(it);
            }
        }
     //
        tripleGraph = SPO;
       // op_S.close();
        System.out.println("sorting indexes.. ");
        System.out.println("spo");
        OPS.sort(1,0);
        System.out.println("op_s");
        op_S.sort(0,-1);
        POS.sort(2,0);
        SPO.sort(1,2);
        indexPool = new IndexesPool(borderTripleMap);
        indexPool.addIndex(IndexesPool.Pso , POS , "Pso");
        indexPool.addIndex(IndexesPool.OPs , OPS , "OPs");
        indexPool.addIndex(IndexesPool.SPo , SPO , "SPo");

//        ArrayList<triple.Vertex> vv = graph.get(vertecesID.get(3));
        System.out.println("done ... errors: " + errCount + " solved:" + errSolved + ", duplicate:" + duplicateCount);
        System.out.println(" error quad processing :" + errQuadProcess + " sucess:" + quadProcess + " err start:" + startErrQuadProcess + " ratio of failure : " + (double) errQuadProcess / (double) quadProcess);

        System.out.println(" the total vetrices = " + verticies.size() + " max code = " + nextCode);
        writeTempIndex(tempop_S,op_S);
        tempop_S = new HashMap<String, ArrayList<Triple>>();
        indexCollection = new IndexCollection();
        //TODO remove ..
        /*if(transporter.getHost().matches("172.20.32.7")) {
            genereteTestBorder2("<Marilyn_Quayle>", "<Barbara_Bush>");
           // genereteTestBorder("<George_W._Bush>", "<Marilyn_Quayle>", "y:hasPredecessor");
           // genereteTestBorder("<George_W._Bush>", "<http://en.wikipedia.org/wiki/Marilyn_Quayle>", "y:describes");
        }else {
            genereteTestBorder2("<George_W._Bush>", "<Barbara_Bush>");
            //genereteTestBorder("<Barbara_Bush>", "<Fahrenheit_9%2F11>", "y:actedIn");
           // genereteTestBorder("<Barbara_Bush>", "<Courting_Condi>", "y:actedIn");
        }
*/



  //      indexCollection.addIndex(SPO, new IndexType(1,0,0));
  //      indexCollection.addIndex(OPS, new IndexType(1,1,0));
  //      indexCollection.addIndex(POS, new IndexType(1,1,1));
  //      indexCollection.addIndexStringKey(op_S, new IndexType(1,0,1));
    }


    private void genereteTestBorder2(String borderVertex , String vertexTokeep){
        int codeBorder = dictionary.get(borderVertex);
        int codeToKeep = dictionary.get(vertexTokeep);

        borderTripleMap.put(dictionary.get(borderVertex), true);

        MyHashMap<Integer, ArrayList<Triple>> Pso = indexPool.getIndex(IndexesPool.Pso);
        MyHashMap<Integer, ArrayList<Triple>> SPo = indexPool.getIndex(IndexesPool.SPo);
        MyHashMap<Integer, ArrayList<Triple>> OPs = indexPool.getIndex(IndexesPool.OPs);

        ArrayList<Triple> list = SPo.get(codeBorder);

        for(int i = 0 ; i < list.size() ; i++){
          Triple t1 = list.get(i);
          if(t1.triples[2] != codeToKeep){
              ArrayList<Triple> listP = Pso.get(t1.triples[1]);
              for(int j = 0; j < listP.size() ; j++){
                  if(listP.get(i).triples[0] == codeBorder && listP.get(i).triples[2] != codeToKeep) {
                      listP.remove(j);
                      j--;
                  }
              }
              if(listP.size() == 0)
                  Pso.remove(t1.triples[1]);
              list.remove(i);
              i--;
          }
        }
        if(list.size() == 0)
            SPo.remove(codeBorder);


        list = OPs.get(codeBorder);

        for(int i = 0 ; i < list.size() ; i++){
            Triple t1 = list.get(i);
            if(t1.triples[0] != codeToKeep){
                ArrayList<Triple> listP = Pso.get(t1.triples[1]);
                for(int j = 0; j < listP.size() ; j++){
                    if(listP.get(i).triples[2] == codeBorder && listP.get(i).triples[0] != codeToKeep) {
                        listP.remove(j);
                        j--;
                    }
                    if(listP.size() == 0)
                        Pso.remove(t1.triples[1]);
                }
                list.remove(i);
                i--;
            }
        }
        if(list.size() == 0)
            OPs.remove(codeBorder);
    }

    private void genereteTestBorder(String borderVertex , String vertexToRemove , String predicateToRemve){

        //create border stuff
//<George_W._Bush> is a border
        //mark it as border in index pool

            borderTripleMap.put(dictionary.get(borderVertex), true);

            MyHashMap<Integer, ArrayList<Triple>> Pso = indexPool.getIndex(IndexesPool.Pso);
            MyHashMap<Integer, ArrayList<Triple>> SPo = indexPool.getIndex(IndexesPool.SPo);
            MyHashMap<Integer, ArrayList<Triple>> OPs = indexPool.getIndex(IndexesPool.OPs);


            int codeRemove = dictionary.get(vertexToRemove);

            ArrayList<Triple> t1 = SPo.remove(codeRemove);
            ArrayList<Triple> t2 = OPs.remove(codeRemove);


            ArrayList<Triple> list = Pso.get(dictionary.get(predicateToRemve));
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).triples[0] == codeRemove) {
                    list.remove(i);
                    i--;
                }
            }
    }


    private static void someTests(){
        long m1 =checkMemory();
        ArrayList<ArrayList<Triple>> testa = new ArrayList();
        HashMap<Long , ArrayList<Triple>> mapInteger = new HashMap<Long , ArrayList<Triple>>();
        HashMap<KeyType , ArrayList<Triple>> mapKey = new HashMap<KeyType , ArrayList<Triple>>();

        for(int i =0 ; i< 1000000 ; i++) {
            long ran = new Random().nextLong();
            KeyType k = new KeyType(ran);
            mapInteger.put(ran , new ArrayList<Triple>());
            mapKey.put(k , new ArrayList<Triple>());
        }

        long startTime = System.nanoTime();
        for(int i =0 ; i< 100000 ; i++) {

            mapKey.get(new KeyType(i));
        }

        long stopTime = System.nanoTime();
        long elapsedTime = (stopTime - startTime) / 1000;
        System.out.println("time:"+elapsedTime);






        startTime = System.nanoTime();
        for(int i =0 ; i< 100000 ; i++) {
            mapInteger.get(i);
        }
        stopTime = System.nanoTime();
        elapsedTime = (stopTime - startTime) / 1000;
        System.out.println("time:"+elapsedTime);



        for(int i =0 ; i< 100000 ; i++) {
            testa.add(new ArrayList<Triple>());
        }
        long m2 = checkMemory();
        System.out.println("memory :"+(m2-m1)/100000.0);
    }

    private boolean buildGraph(int[] code,ArrayList<Vertex> v) {
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

    private void addToVertexGraph(int [] code){
        if(verticies == null)
            verticies = new HashMap<Integer, VertexGraph>();
        //this code to create an extra graph vertex which should be used in the graph algo, obviously this is redenednt situ to the created vertext in the latter code block
        VertexGraph savedVertex = verticies.get(code[0]);
        if (savedVertex == null) {
            savedVertex = new VertexGraph(code[0]);
            verticies.put(code[0], savedVertex);
        }
        savedVertex.addEdge(code[2], code[1]);
        if (verticies.get(code[2]) == null)
            verticies.put(code[2], new VertexGraph(code[2]));
        //end of grpah vertex creation
    }

    private void printIndexesStat(){
       /* indexCollection.printStat();
        IndexCollection.printIndexStat(verticies);
        IndexCollection.printIndexStat(graph);
        IndexCollection.printIndexStat(dictionary);
        IndexCollection.printIndexStat(reverseDictionary);
        IndexCollection.printIndexStat(tripleGraph);*/
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
        dictionary = new Dictionary("dictionary_int");
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
        String [] linet = new String[1];
        linet[0] = line;
        triple[0] = stripItem(linet);
        triple[1] = stripItem(linet);
        triple[2] = stripItem(linet);
        if(triple[2] != null)
            triple[2] = triple[2];
        if(triple[0] != null && triple[1] != null && triple[2] != null)
            return triple;
        errCount++;
        return null;
    }



    private String stripItem(String []linet){
        String line = linet[0].trim();
        String item = null;
        char startChar = line.charAt(0);
        switch (startChar){
            case '\"':
                line = line.substring(1);
                for (int i = 1; i < line.length(); i++){
                    char c = line.charAt(i);
                    if(c == '\"' && line.charAt(i-1) != '\\' ) {
                        item = line.substring(0, i + 1);
                        line = line.substring(i+1);
                        linet[0] = line;
                        return "\""+item;
                    }
                }
                if(line.startsWith("\""))
                    return "\""+"\"";
             return null;

            case '<':
                item = line.substring(0,line.indexOf('>')+1);
                line = line.substring(line.indexOf('>')+1);
                linet[0] = line;
                return item;
            case '_':
                item = line.substring(0,line.indexOf(' ')+1);
                line = line.substring(line.indexOf(' ')+1);
                linet[0] = line;
                return item;
        }
        String []arr = line.split(":");
        if(arr.length > 0){
            for(int i = 0 ; i < header.size() ; i++){
                if(prefix.containsKey(arr[0])){
                    char endChar;
                    if(line.contains(" "))
                        endChar = ' ';
                    else if(line.contains("}"))
                        endChar = '}';
                    else if(line.contains("."))
                        endChar = '.';
                    else
                        return null;
                    item = line.substring(0,line.indexOf(endChar)+1);
                    line = line.substring(line.indexOf(endChar)+1);
                    linet[0] = line;
                    return item.trim();

                }
                    return line;
            }
        }
        return null;
    }

    private int errQuadProcess = 0;
    private int startErrQuadProcess = 0;
    private int quadProcess = 0;

    private String[] getTripleFromQuadLine(String line) {
        if(line.contains("<http://purl.org/rss/1.0/modules/content/encoded>"))
            line.contains("");
        String t = line.replace("qqq","");
        if (!line.startsWith("<") && !line.startsWith("_:")) {
            errQuadProcess++;
            startErrQuadProcess++;
            return null;
        }
        String[] triple = new String[3];
        String [] linet = new String[1];
        linet[0] = line;
        triple[0] = stripItem(linet);
        triple[1] = stripItem(linet);
        triple[2] = stripItem(linet);
        if(triple[2] != null)
            triple[2] = triple[2]+'.';
        if(triple[0] != null && triple[1] != null && triple[2] != null)
            return triple;
        errQuadProcess++;
        return null;
    }

    private String[] getTripleFromQuadLine_old(String line) {
        String t = line.replace("qqq","");
        if (!line.startsWith("<") && !line.startsWith("_:")) {
            errQuadProcess++;
            startErrQuadProcess++;
            return null;
        }
        String[] triple = new String[3];
        try {
            try {
                triple[0] = line.substring(line.indexOf("<"), line.indexOf(">") + 1);
                if (line.length() - line.replace(triple[0], "").length() > triple[0].length() + 2)
                    line = line.replaceFirst(triple[0], "");
                else
                    line = line.replace(triple[0], "");

            } catch (PatternSyntaxException e) {
                errQuadProcess++;
                e.printStackTrace();
                return null;
            }
            try {
                int from, to;
                if (line.contains("<")) {
                    from = line.indexOf("<");
                    to = line.indexOf(">");
                    triple[1] = line.substring(from + 1, to); //without the '<' and '>'
                    // triple[1] = triple[1] + "<pred:" + triple[1] + ">";
                    triple[1] = "<" + triple[1] + ">"; //I decideted to put it without prefix jan 2019

                    line = line.replaceFirst(triple[1], "");
                } else {
                    errQuadProcess++;
                    return null;
                }
                line = line.trim();
                if (line.startsWith("<")) {
                    triple[2] = line.substring(0, line.indexOf(">")+1);
                } else {
                    from = line.indexOf('\"');
                    to = line.lastIndexOf('\"');
                    if (from >= 0 && to >= 0) {
                        triple[2] = line.substring(from, to + 1);
                        line = line.replace(triple[2], "");
                    }
                    else
                        return null;
                }
                /*else {
                    from = line.indexOf("<");
                    to = line.indexOf(">");
                    triple[2] = line.substring(from, to + 1);
                    line = line.replace(triple[2], "");
                }*/
                triple[2] = triple[2] + '.';

            } catch (StringIndexOutOfBoundsException e) {
                errQuadProcess++;
                e.printStackTrace();
                System.err.println(triple[0] + "   " + triple[1] + "     " + triple[2]);
                return null;
            }
            if (triple[0] == null || triple[1] == null || triple[2] == null) {
                errQuadProcess++;
                return null;
            }
            quadProcess++;
            return triple;
        } catch (Exception e) {
        }
        errQuadProcess++;
        return null;

    }


    private void addTosp_OIndex(Triple triple) {
        if (sp_O == null)
            sp_O = new MyHashMap("sp_O");
        int code[] = triple.triples;
        String key = code[0] + "" + code[1];
        addToIndex(sp_O, triple, key);

    }


    private void addToOp_SIndex(Triple triple) {
        if (op_S == null)
            op_S = new MyHashMap("op_S",new IndexType(1,0,0) , false);
        int code[] = triple.triples;
        String key = code[2] + "" + code[1];
        op_S.addTripleLazy(key,triple);
        //addToIndex(op_S, triple, key);
        //addToIndexTemp(tempop_S,triple,key);
    }


    private void writeTempIndex(HashMap temp , MyHashMap persist){
        Iterator it = temp.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            String key = (String) pair.getKey();
            ArrayList val = (ArrayList) pair.getValue();
            persist.put(key , val);
        }
    }

    private void addToIndex(MyHashMap<String, ArrayList<Triple>> index, Triple triple, String key) {
        if (index.containsKey(key)) {
            index.appendToTripleList(key,triple);
        } else {
            ArrayList<Triple> list = new ArrayList();
            list.add(triple);
            index.put(key, list);
        }
    }

    private void addToIndexTemp(HashMap<String, ArrayList<Triple>> index, Triple triple, String key) {
        if (index.containsKey(key)) {
            index.get(key).add(triple);
        } else {
            ArrayList<Triple> list = new ArrayList();
            list.add(triple);
            index.put(key, list);
        }
    }


    private void addToPOSIndex(Triple triple) {
        if (POS == null)
            POS = new MyHashMap("POS");
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
            OPS = new MyHashMap("OPS" , new IndexType(1,1,0) );
        OPS.addTripleLazy(code[2] , triple);
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

        putStringTripleInQueries(queries);
        writeQueriesToFile(queries);

        return queries;
    }



    private void putStringTripleInQueries(ArrayList<QueryStuff.Query> queries) {
        for (int i = 0; i < queries.size(); i++) {
            queries.get(i).findStringTriple(reverseDictionary);
            queries.get(i).setQuerySPARQL(reverseDictionary, prefix, verticies);
        }
        //  writeQueriesToFile(queries);
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

    private void writeQueriesToFile(ArrayList<Query> queries) {
        BufferedWriter bw = null;
        FileWriter fw = null;
        try {
            fw = new FileWriter("queriesFile");
            bw = new BufferedWriter(fw);
            for (int i = 0; i < queries.size(); i++) {
                bw.write(queries.get(i).queryFrquency + "," + queries.get(i).SPARQL);
                bw.newLine();
                bw.write(queries.get(i).queryFrquency + "," + queries.get(i).partitionString);
                bw.newLine();
                bw.write(queries.get(i).queryFrquency + "," + queries.get(i).answerString);
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


    private void buildSppIndex() {
        SPxP = new MyHashMap("SPxP");
        for (int i = 0; i < vertecesID.size(); i++) {
            int s = vertecesID.get(i);
            ArrayList<Triple> connectedTriples = SPO.get(s);
            if (connectedTriples == null)
                continue;
            for (int j = 0; j < connectedTriples.size(); j++) {
                Triple triple1 = connectedTriples.get(j);
                int o = triple1.triples[2];
                int p1 = triple1.triples[1];
                if (!SPO.containsKey(o))
                    continue;
                ArrayList<Triple> connectedTriples2 = SPO.get(o);
                for (int k = 0; k < connectedTriples2.size(); k++) {
                    Triple Triple = connectedTriples2.get(k);
                    int p2 = Triple.triples[1];
                    String key = s + "" + p1 + "" + p2;
                    addToIndex(SPxP, triple1, key);
                    addToIndex(SPxP, Triple, key);
                        /*
                        ArrayList<Triple> tripleList = SPxP.get(key);
                        if(tripleList == null)
                            tripleList = new ArrayList();
                        tripleList.add(triple1);
                        tripleList.add(Triple);
                        SPxP.put(key , tripleList);*/
                }
            }
        }
    }



    private void buildOppIndex() {
        //iterate over the OPS index
        int totalSize = OPS.size()+1;
        int cnt = 0;
        //first iterate over OPS cache
        Iterator it = OPS.cacheEntrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            Integer O = (Integer) pair.getKey();
            ArrayList<Triple> list1 = (ArrayList<Triple>)pair.getValue();
            addToOppIndex(O,list1);
            cnt++;
            if(cnt % 100000 == 0) {
                System.out.print(" , " + cnt * 100 / totalSize);
                checkMemory();
            }
        }
        //then iterate over OPS disk
        it = OPS.fastEntrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            Integer O = (Integer) pair.getKey();
            ArrayList<Triple> list1 = OPS.deCompressedHyprid((String)pair.getValue());
            addToOppIndex(O,list1);
            cnt++;
            if(cnt % 100000 == 0){
                System.out.print(" , "+cnt*100/totalSize);
                checkMemory();

            }
        }
        //writeTempIndex(tempOPxP,OPxP);
      //  tempOPxP = null;
        //  indexCollection.addIndexStringKey(OPxP , new IndexType(0,0,0));
    }


    private int oppsCurrentCount = 0;
    private void addToOppIndex(Integer O ,ArrayList<Triple> list1) {
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
                OPxP.addTripleLazy(key,triple1);
                OPxP.addTripleLazy(key,Triple);
               // addToIndexTemp(OPxP, triple1, key);
               //addToIndexTemp(OPxP, Triple, key);
                //addToIndex(OPxP, triple1, key);
                // addToIndex(OPxP, Triple, key);

                if(oppsCurrentCount % 100000 == 0) {
                    for (int m = 0; m < 50; m++)
                        System.out.println();
                    System.out.flush();
                    System.out.println("adding triple "+oppsCurrentCount/1000 +" k, writting buffer size = " + OPxP.getWritingThreadBufferSize()/1000 +" k");
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
        if(queryKeys.size()>40)
            return;
        int ch = new Random().nextInt(100) + 1;
        if(ch == 10)
            queryKeys.add(key);

    }


    //TODo iterate over file set also?
    private void buildOppIndex2() {
        //iterate over the OPS index
        Iterator it = OPS.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            Integer O = (Integer) pair.getKey();
            ArrayList<Triple> list1 = (ArrayList<Triple>) pair.getValue();
            for (Triple triple1 : list1) {
                int P1 = triple1.triples[1];
                int s = triple1.triples[0];
                //check each s as o in the OPS
                ArrayList<Triple> list2 = OPS.get(s);
                if (list2 == null)
                    continue;
                for (Triple Triple : list2) {
                    int P2 = Triple.triples[1];
                    String key = O + "" + P1 + "" + P2;
                    addToIndex(OPxP, triple1, key);
                    addToIndex(OPxP, Triple, key);
                }
            }
        }
      //  indexCollection.addIndexStringKey(OPxP , new IndexType(0,0,0));
    }


    private void generateQueryGraph(ArrayList<Query> queries) {
        queryGraph = new GlobalQueryGraph(queries, 4, POS, SPO, OPS, verticies);
        queryGraph.genrate();


    }


    private HashMap<Integer, Integer> codeToPartutpartitionMap;
    private HashMap<Integer, Integer> borderCodesPartutMap;

    private void writePartutPartitions(int partitionCount) {

        queryGraph.assignPartut(partitionCount);
        //  assignPartutPartionInVertices(2);

        codeToPartutpartitionMap = new HashMap();
        borderCodesPartutMap = new HashMap();

        BufferedWriter[] outBuff = new BufferedWriter[partitionCount];
        FileWriter[] outfw = new FileWriter[partitionCount];
        int[] partitionsSize = new int[partitionCount];
        for (int j = 0; j < partitionCount; j++)
            partitionsSize[j] = 0;

        ArrayList<String>[] out_ext_list = new ArrayList[partitionCount];
        // FileWriter []  outfw_ext = new FileWriter[partitionCount];
        try {
            for (int i = 0; i < partitionCount; i++) {
                outfw[i] = new FileWriter("/home/ahmed/rdfToMetisoutput/out_partut" + i + ".n3");
                outBuff[i] = new BufferedWriter(outfw[i]);
                out_ext_list[i] = new ArrayList();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        for (int r = 0; r < partitionCount; r++)
            for (int s = 0; s < header.size(); s++) {
                try {
                    outBuff[r].write(header.get(s) + "");
                    outBuff[r].newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }


        for (int i = 0; i < queryGraph.getAnnomizedTriples().size(); i++) {

            AnnomizedTriple annomizedTriple = queryGraph.getAnnomizedTriples().get(i);
            ArrayList<Triple> fragmentTriples = annomizedTriple.fragment.triples;
            int partutPartition = annomizedTriple.partutAssignedPartition;
            for (int j = 0; j < fragmentTriples.size(); j++) {
             /*  boolean isAlreadyBorder =  borderCodesPartutMap.get(fragmentTriples.get(j).triples[0]) != null;
               if(isAlreadyBorder)
                   continue;
               QueryStuff.VertexGraph srcVertex = this.verticies.get(fragmentTriples.get(j).triples[0]) ;
               QueryStuff.VertexGraph destVertex = this.verticies.get(fragmentTriples.get(j).triples[2]) ;
               if(srcVertex)
               Integer foundPartition = codeToPartutpartitionMap.get(fragmentTriples.get(j).triples[0]);
               if(foundPartition == null){
                   codeToPartutpartitionMap.put(fragmentTriples.get(j).triples[0] ,annomizedTriple.partutAssignedPartition );
               }else if(foundPartition != annomizedTriple.partutAssignedPartition ){
                   borderCodesPartutMap.put(fragmentTriples.get(j).triples[0] , fragmentTriples.get(j).triples[0]);
               }
               */
                String tripleLine = reverseDictionary.get(fragmentTriples.get(j).triples[0]) + " " + reverseDictionary.get(fragmentTriples.get(j).triples[1]) + " " + reverseDictionary.get(fragmentTriples.get(j).triples[2]) + " .";
                tripleToPartutPartitionMap.put(fragmentTriples.get(j).triples[0] + "p" + fragmentTriples.get(j).triples[1] + "p" + fragmentTriples.get(j).triples[2], partutPartition);

                try {
                    outBuff[partutPartition].write(tripleLine);
                    outBuff[partutPartition].newLine();
                    partitionsSize[partutPartition]++;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


        }

        HashMap<Integer, ArrayList<String>> ooohAnotherMap = new HashMap();
        Iterator it = tripleToPartutPartitionMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            Integer partNo = (Integer) pair.getValue();

            try {
                int s = Integer.valueOf(((String) pair.getKey()).split("p")[0]);
                int p = Integer.valueOf(((String) pair.getKey()).split("p")[1]);
                int o = Integer.valueOf(((String) pair.getKey()).split("p")[2]);

                String tripleLine = reverseDictionary.get(s) + " " + reverseDictionary.get(p) + " " + reverseDictionary.get(o);
                if (!tripleLine.endsWith("."))
                    tripleLine = tripleLine + " .";
                //int partition = (int) (Math.random() * partitionCount);
                if (partNo == -1) {
                    ArrayList<String> list = ooohAnotherMap.get(p);
                    if (list == null) {
                        list = new ArrayList();
                        ooohAnotherMap.put(p, list);
                    }
                    list.add(tripleLine);
                    continue;
                }
                outBuff[partNo].write(tripleLine);
                outBuff[partNo].newLine();
                partitionsSize[partNo]++;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }


        //now distiribute all the triples which have no fragment
        it = ooohAnotherMap.entrySet().iterator();
        while (it.hasNext()) {
            //first find the partition with current lower size:
            int toPartition = 0;
            for (int j = 0; j < partitionCount; j++) {
                if (partitionsSize[j] < partitionsSize[toPartition])
                    toPartition = j;
            }
            //now write the triple to this partition
            Map.Entry pair = (Map.Entry) it.next();
            Integer p = (Integer) pair.getKey();
            ArrayList<String> tripleLines = (ArrayList<String>) pair.getValue();
            for (int i = 0; i < tripleLines.size(); i++) {
                String line = tripleLines.get(i);
                try {
                    outBuff[toPartition].write(line);
                    outBuff[toPartition].newLine();
                    partitionsSize[toPartition]++;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        try {
            for (int j = 0; j < partitionCount; j++) {
                if (outBuff[j] != null)
                    outBuff[j].close();
                if (outfw[j] != null)
                    outfw[j].close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private void assignPartutPartionInVertices(int partitionCount) {
        // we first pass over all the triples of the fragments and assigns the partition number to it.
        for (int i = 0; i < queryGraph.getAnnomizedTriples().size(); i++) {
            AnnomizedTriple annomizedTriple = queryGraph.getAnnomizedTriples().get(i);
            ArrayList<Triple> fragmentTriples = annomizedTriple.fragment.triples;
            for (int j = 0; j < fragmentTriples.size(); j++) {
                Triple triple = fragmentTriples.get(j);
                VertexGraph vertex = verticies.get(triple.triples[0]);
                if (vertex.partutPartitionsMap == null)
                    vertex.partutPartitionsMap = new HashMap();
                vertex.partutPartitionsMap.put(annomizedTriple.partutAssignedPartition, annomizedTriple.partutAssignedPartition);
            }

        }

        // now we assign all the triples which does not beint to a fragment

        for (int j = 0; j < verticies.size(); j++) {
            VertexGraph vertex = verticies.get(j);
            if (vertex.partutPartitionsMap == null) {
                int partition = (int) (Math.random() * partitionCount);
                vertex.partutPartitionsMap = new HashMap();
                vertex.partutPartitionsMap.put(partition, partition);
                for (int k = 0; k < vertex.edgesVertex.size(); k++) {
                    HashMap map = verticies.get(vertex.edgesVertex.get(k)).partutPartitionsMap;
                    if (map == null)
                        map = new HashMap();
                    map.put(partition, partition);
                    verticies.get(vertex.edgesVertex.get(k)).partutPartitionsMap = map;
                }
            }

        }
    }

    public void convertNqToN3(String sourceFile, String destFile ,int from , int to , int mult) {
        String path ;
        for(int i=from; i<=to ; i++){
            path = sourceFile +(i*mult);
            convertNqToN3(path,destFile,true);
        }
    }
    //this method only trnasofrm sourcefile in quad format to destFile in n3 format
    public void convertNqToN3(String sourceFile, String destFile , boolean append) {
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
            fw = new FileWriter(destFile,append);
            bw = new BufferedWriter(fw);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        int duplicateCount = 0;
        int count = 0;
        while (it.hasNext() ) {
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


    private ArrayList<String> readFromQueryFile(){
        String path = "manyQueriesFile";
        System.out.println("reading dictionary from temp file");
        ArrayList<String> res = new ArrayList<>();
        File file = new File(path);
        LineIterator it = null;
        try {
            it = FileUtils.lineIterator(file);
            while (it.hasNext()) {
                String line = it.nextLine();
                res.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return res;
    }



    private void listenToQuery() {

        String testquery = "select ?x1 ?x2 ?x3 ?x4  where {?x3 y:describes ?x2.?x2 y:created ?x1.?x1 y:hasSuccessor ?x4.?x4 rdfs:label ?x5}";
        new Query(dictionary, testquery,indexPool , transporter);//warm up!!
        Scanner scanner = new Scanner(System.in);
        InterExecutersPool executersPool = new InterExecutersPool(7);
        queryWorkersPool = new QueryWorkersPool(dictionary ,transporter , indexPool);
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
                    String s = query.replace("g","").trim();
                    double memPercent = 0;
                    if(!s.equals(""))
                        memPercent = Double.valueOf(s);
                    if(memPercent > 1)
                        memPercent = memPercent/100;
                    ArrayList<String> HeaveyQueries = QueryGenrator.buildFastHeavyQueryZero(reverseDictionary , OPS , 4 , 4);
                    writePalinQueriesToFile(HeaveyQueries);
                    System.out.println("done ..queries written to file..");
                    continue;
                }
                if(query.matches("f")){
                    ArrayList<String> queriesStr = readFromQueryFile();
                    queryWorkersPool.addManyQueries(queriesStr);
                   /* for(int i = 0 ; i < queriesStr.size() ; i++){
                        int queryNo = queryWorkersPool.addManyQueries(queriesStr.get(i) , i == queriesStr.size()-1);
                        transporter.sendQuery(queriesStr.get(i) , queryNo);
                    }*/
                    continue;
                }
                //StringBuilder extTime = new StringBuilder();
                int queryNo = queryWorkersPool.addSingleQuery(query);  ;
                transporter.sendQuery(query , queryNo);


               /*long startTime = System.nanoTime();
                Query spQuery = new Query(dictionary, query,indexPool,transporter);
                long parseTime = System.nanoTime();
                spQuery.findQueryAnswer();
                long stopTime = System.nanoTime();
                long elapsedTimeS = (stopTime - startTime) / 1000;



                spQuery.printAnswers(reverseDictionary , false);

                System.out.println("time to execute qeury single thread:" + elapsedTimeS + " micro seconds,"+" time to OPxP "+extTime+" Ms, parse time:"+ (parseTime - startTime) / 1000+" Ms");
*/
            }catch (Exception e){
                System.err.println("unable to parse query..");
                e.printStackTrace();
            }
        }
    }

    private void finish() {
        if(OPS != null)
            OPS.close();
        if(OPxP != null)
            OPxP.close();
        if(op_S != null)
            op_S.close();
        if(dictionary != null)
            dictionary.close();
    }


    private static long checkMemory(){
        long rem =  Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long max = Runtime.getRuntime().maxMemory();
        System.out.println(" app used memory: "+rem/1000000000 + " GB");
        System.out.println(" app used memory: "+rem/1000 + " KB");
        if((max-rem)/1000 < 2000000) {
            System.out.println(" Low memory detected, exiting ... ");
            //finish();
           // System.exit(1);
        }
        return rem/1000; //kB
    }




}
