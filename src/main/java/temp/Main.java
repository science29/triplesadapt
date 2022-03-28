package temp;

import triple.Triple;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import triple.Vertex;


import java.io.*;
import java.util.*;
import java.util.regex.PatternSyntaxException;


public class Main {

   /* private HashMap<Long, ArrayList<Vertex>> graph = new HashMap();

    private HashMap<Long, ArrayList<Triple>> tripleGraph = new HashMap();//duplicate with graph , remove one!
    private ArrayList<Long> vertecesID = new ArrayList();
    private HashMap<Long, QueryStuff.VertexGraph> verticies;
    private HashMap<String, Long> dictionary = new HashMap();
    private HashMap<Long, String> reverseDictionary = new HashMap();
    private HashMap<Integer, ArrayList<QueryStuff.VertexGraph>> distanceVertex;
    private long edgesCount = 0;
    private static final int PARTITION_COUNT = 2;

    private HashMap<Long, ArrayList<Triple>> POS;
    private HashMap<Long, ArrayList<Triple>> SPO;
    private HashMap<Long, ArrayList<Triple>> OPS;
    private HashMap<String, Integer> tripleToPartutPartitionMap;
    private QueryStuff.GlobalQueryGraph queryGraph;


    static boolean partutSupport = false;

    final static boolean includeCompressed = false;

    final static boolean weighting = false;
    final static boolean includeFragments = true;
    final static boolean quad = true;
    //final static String dataSetPath = "/home/ahmed/download/btc-data-3.nq";
    final static String dataSetPath = "/home/keg/Downloads/rexo.nq";
    final static String outPutDirName = "bioportal";


    public static void main(String[] args) {


        System.out.println("starting ..");
        temp.Main o = new temp.Main();
        o.convertNqToN3("/afs/informatik.uni-goettingen.de/user/a/aalghez/Desktop/RDF3X netbean/rdf3x-0.3.7/bin/btc-2009-small.nq", "btc-2009-small.n3");
        System.out.println("done converting..");
        byte[] a =  new byte[1000];
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //""
        // "/home/ahmed/download/yago2_core_20101206.n3"
        //String dataSetPath = "/afs/informatik.uni-goettingen.de/user/a/aalghez/Desktop/RDF3X netbean/rdf3x-0.3.7/bin/yago_utf.n3";

        o.process(dataSetPath, quad);
        System.out.println("generating queries .. ");
        ArrayList<QueryStuff.Query> queries = o.generateQueries();


        // System.out.println("setting dynamic weigths .. ");
        o.setDynamicWeights(queries);


        System.out.println("writting the 1st metis input file");

        o.writeFile(weighting);
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

        System.out.println("Writting queries to file");
        o.putStringTripleInQueries(queries);
        o.writeQueriesToFile(queries);

    }


    private void setDynamicWeights(ArrayList<QueryStuff.Query> queries) {
        QueryStuff.GlobalQueryGraph.setDynamicWeights(queries, tripleGraph, graph, POS);
    }

    private void clean() {
        dictionary = null;
        reverseDictionary = null;
    }

    private void setPartitionNumberInVertices(LineIterator it) {
        long count = 1;
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

    private HashMap<Long, QueryStuff.VertexGraph> toBeCheckedVertexes; // this is the list of non boraders vertex that need to be checked if they have been written before the border ones

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

            long count = 1;
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

                        *//*if(p1 == -1 || p2 == -1)
                            break;
                        if (p1 != p2)*//*
                        boolean bsource = false;//isBorderMap(count);
                        boolean bdest = v.get(i).isBorderMap;//isBorderMap(v.get(i).v);
                        if (bsource || bdest) {
                            if (!bsource && !verticies.get(count).writtenToFile) // it is not border and not been written yet
                                toBeCheckedVertexes.put(count, verticies.get(count)); // add to chekmap
                            if (!bdest && !verticies.get(v.get(i).v).writtenToFile) // it is not border and not been written yet
                                toBeCheckedVertexes.put(count, verticies.get(v.get(i).v)); // add to chekmap
                            triple = triple + " .";
                            out_ext_list[n].add(triple);
                            compressed_ext_list[n].add(triple);
                            boolean added = verticies.get(v.get(i).v).addLinkInPartition(p2);
                           *//* if (added && reverseDictionary.get(v.get(i).v).contains("<") && reverseDictionary.get(v.get(i).v).contains(">")) {
                                String InBorderTriple = reverseDictionary.get(v.get(i).v) + " " + "x:isRefferedBy" + " " + "\"" + p2 + "\"";
                                outBuff[p1].write(InBorderTriple + " .");
                                outBuff[p1].newLine();
                            }*//*
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
                QueryStuff.VertexGraph vx = (QueryStuff.VertexGraph) pair.getValue();
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
                    long g = 2;
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

                HashMap<String, Long> writtenTripleFragment = new HashMap();
                //write the fragments if required:
                if (includeFragments) {
                    if (queryGraph == null) {
                        System.err.println("no global query graph found, consider buliding the query graph first");
                        System.exit(1);
                    }
                    ArrayList<QueryStuff.AnnomizedTriple> fragmentsTriple = queryGraph.getAnnomizedTriples();
                    int nonBorderErr = 0;
                    int total = 0;
                    for (int i = 0; i < fragmentsTriple.size(); i++) {
                        QueryStuff.AnnomizedTriple anmoizedTriple = fragmentsTriple.get(i);
                        ArrayList<Triple> triples = anmoizedTriple.fragment.triples;
                        for (int j = 0; j < triples.size(); j++) {
                            total++;
                            QueryStuff.VertexGraph vs = verticies.get(triples.get(j).triples[0]);
                            int sourcePartitionNr = vs.partitionNumber;
                            QueryStuff.VertexGraph vd = verticies.get(triples.get(j).triples[2]);

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
                        long g = 2;
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

    private boolean isBorderMap(long vid) {
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
                    ves.get(j).isBorderMap = true;
            }
        }
        System.out.println("done ");
    }
*//*
    private void setVertexVisted(long v) {
       QueryStuff.VertexGraph vertexGraph = verticies.get(v);
       if(vertexGraph.dist == -1)
           vertexGraph.dist = -2;
    }*//*

    private void setDistVervtices(int maxDist) {
        distanceVertex = new HashMap();
        distanceVertex.put(0, new ArrayList());
        //set distane zero
        for (int i = 0; i < vertecesID.size(); i++) {
            QueryStuff.VertexGraph vertexGraph = verticies.get(vertecesID.get(i));
            for (int j = 0; j < vertexGraph.edgesVertex.size(); j++) {
                long toVertexID = vertexGraph.edgesVertex.get(j);
                QueryStuff.VertexGraph toVertex = verticies.get(toVertexID);
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
            ArrayList<QueryStuff.VertexGraph> newDistList = new ArrayList();
            distanceVertex.put(i, newDistList);
            ArrayList<QueryStuff.VertexGraph> prevVertices = distanceVertex.get(i - 1);
            for (int j = 0; j < prevVertices.size(); j++) {
                QueryStuff.VertexGraph edgeVert = prevVertices.get(j);
                for (int k = 0; k < edgeVert.edgesVertex.size(); k++) {
                    long vid = edgeVert.edgesVertex.get(k);
                    QueryStuff.VertexGraph v = verticies.get(vid);
                    if (v.dist == -1) {
                        v.dist = edgeVert.dist + 1;
                        newDistList.add(v);
                    }
                }
            }
            System.out.println("setting " + i + " dist : " + newDistList.size() + " vertecies");
        }

    }


    private ArrayList<Long> findSubjectVertexList() {
        ArrayList<Long> res = new ArrayList();
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
      *//*      if (str.matches("")){
                System.err.println("error: empty line detected");
                System.exit(1);
            }*//*

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
        System.out.println("done writing...");
    }

    private long edgesWritten = 0;
    private HashMap<Long, HashMap<Long, Vertex>> graphMap = new HashMap();
    private HashMap<Long, String> tempIntToStringMap = new HashMap();
    double progressRatio = 0;

    private String buildVertixString(ArrayList<Vertex> vees, long currentVertexID, boolean weighting) {
        if (vees.size() == 0)
            return "";
        if (vees.size() > 100000)
            return "";
        //String res = vees.get(0).v+" "+(vees.get(0).weight1 + vees.get(0).weight2)+"";
        String res = "";
        StringBuilder stringBuilder = new StringBuilder();
        boolean first = true;
        // HashMap<Long, Long> map = new HashMap();
        for (int i = 0; i < vees.size(); i++) {
            int weight = 1;
            if (!weighting) {
                if (vees.get(i).e == -1) {
                    ArrayList<Vertex> other = this.graph.get(vees.get(i).v);
                    HashMap<Long, Vertex> otherMap = graphMap.get(vees.get(i).v);
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
                tempIntToStringMap.put((long) weight, weightStr);
            }
            *//*
            String destVertexID = tempIntToStringMap.get(vees.get(i).v);
            if(destVertexID == null) {
                destVertexID =  vees.get(i).v + "" ;
                tempIntToStringMap.put(vees.get(i).v, destVertexID);
            }*//*
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
                System.out.println("writing progress:" + (100 * progressRatioN));
                progressRatio = progressRatioN;
            }
        }
        return stringBuilder.toString();
    }

    HashMap<String, String> prefix = new HashMap<String, String>();
    ArrayList<String> header;

    public void process(String filePath, boolean quad) {
        tripleToPartutPartitionMap = new HashMap();
        header = new ArrayList();
        if (verticies == null)
            verticies = new HashMap();
        long[] code = new long[3];
        long nextCode = 1;
        long nextPredicateCode = 1000000000;
        Integer errCount = 0;
        Integer errSolved = 0;
        File file = new File(filePath);
        LineIterator it = null;
        try {
            it = FileUtils.lineIterator(file, "US-ASCII");
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        int duplicateCount = 0;
        try {
            int count = 0;
            while (it.hasNext()) {
                String lineTemp;
                if (count % 10000 == 0) {
                    System.out.println("processing line " + count);
                }
                count++;
                //     if(count == 100000)
                //      break;
                String line = it.nextLine();
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
                ArrayList<Vertex> v = null;
                for (int i = 0; i < 3; i++) {
                    if (dictionary.containsKey(triple[i])) {
                        code[i] = dictionary.get(triple[i]);
                        // v = graph.get(code[i]);
                    } else {
                        if (i != 1) {
                            code[i] = nextCode;
                            nextCode++;
                            dictionary.put(triple[i], code[i]);
                            reverseDictionary.put(code[i], triple[i]);
                            v = new ArrayList();
                            vertecesID.add(code[i]);
                            graph.put(code[i], v);
                        } else {
                            code[i] = nextPredicateCode;
                            nextPredicateCode++;
                            dictionary.put(triple[i], code[i]);
                            reverseDictionary.put(code[i], triple[i]);
                        }
                    }
                }
                try {
                    //this code to create an extra graph vertex which should be used in the graph algo, obviously this is redenednt situ to the created vertext in the latter code block
                    QueryStuff.VertexGraph savedVertex = verticies.get(code[0]);
                    if (savedVertex == null) {
                        savedVertex = new QueryStuff.VertexGraph(code[0]);
                        verticies.put(code[0], savedVertex);
                    }
                    savedVertex.addEdge(code[2], code[1]);
                    if (verticies.get(code[2]) == null)
                        verticies.put(code[2], new QueryStuff.VertexGraph(code[2]));
                    //end of grpah vertex creation

                    v = graph.get(code[0]);
                    boolean duplicateFlag = false;
                    for (int i = 0; i < v.size(); i++)
                        if (v.get(i).v == code[2]) {
                            duplicateCount++;
                            duplicateFlag = true;
                            break;
                        }
                    if (duplicateFlag)
                        continue;
                    v.add(new Vertex(code[2], code[1]));
                    edgesCount++;
                    v = graph.get(code[2]);
                    v.add(new Vertex(code[0], -1));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                //build the tripe graph
                Triple tripleObj = new Triple(code[0], code[1], code[2]);
                if (!tripleGraph.containsKey(code[0]))
                    tripleGraph.put(code[0], new ArrayList<Triple>());
                tripleGraph.get(code[0]).add(tripleObj);
                if (partutSupport) {
                    tripleToPartutPartitionMap.put(code[0] + "p" + code[1] + "p" + code[2], -1);
                }
                addToPOSIndex(code);
                addToSPOIndex(code);
                addToOPSIndex(code);
            }
        } finally {
            LineIterator.closeQuietly(it);
        }
        ArrayList<Vertex> vv = graph.get(vertecesID.get(3));
        System.out.println("done ... errors: " + errCount + " solved:" + errSolved + ", duplicate:" + duplicateCount);
        System.out.println(" error quad processing :" + errQuadProcess + " sucess:" + quadProcess + " err start:" + startErrQuadProcess + " ratio of failure : " + (double) errQuadProcess / (double) quadProcess);

        System.out.println(" the total vetrices = " + verticies.size() + " max code = " + nextCode);
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
        dictionary = new HashMap();
        reverseDictionary = new HashMap();
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
            Long key = Long.valueOf(strArr[1]);
            dictionary.put(strArr[0], key);
            reverseDictionary.put(key, strArr[0]);
        }
        System.out.println("done");
    }

    private String[] getTripleFromTripleLine(String line, Integer errCount, Integer errSolved) {
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
                if (line.contains("<")) {
                    from = line.lastIndexOf("<");
                    to = line.lastIndexOf(">") + 1;
                    triple[2] = line.substring(from, to);
                } else {
                    from = line.indexOf('\"');
                    to = line.lastIndexOf('.');
                    triple[2] = line.substring(from, to + 1);
                }

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

        return triple;

    }


    private int errQuadProcess = 0;
    private int startErrQuadProcess = 0;
    private int quadProcess = 0;

    private String[] getTripleFromQuadLine(String line) {
        if (!line.startsWith("<")) {
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


                from = line.indexOf('\"');
                to = line.lastIndexOf('\"');
                if (from >= 0 && to >= 0) {
                    triple[2] = line.substring(from, to + 1);
                    line = line.replace(triple[2], "");
                } else {
                    from = line.indexOf("<");
                    to = line.indexOf(">");
                    triple[2] = line.substring(from, to + 1);
                    line = line.replace(triple[2], "");
                }
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

    private void addToPOSIndex(long[] code) {
        if (POS == null)
            POS = new HashMap();
        Triple triple = new Triple(code[0], code[1], code[2]);
        if (POS.containsKey(code[1])) {
            POS.get(code[1]).add(triple);
        } else {
            ArrayList<Triple> list = new ArrayList();
            list.add(triple);
            POS.put(code[1], list);
        }
    }

    private void addToSPOIndex(long[] code) {
        if (SPO == null)
            SPO = new HashMap();
        Triple triple = new Triple(code[0], code[1], code[2]);
        if (SPO.containsKey(code[0])) {
            SPO.get(code[0]).add(triple);
        } else {
            ArrayList<Triple> list = new ArrayList();
            list.add(triple);
            SPO.put(code[0], list);
        }
        int a = 2;
        if (OPS != null && OPS.containsKey(code[0]))
            a = 0;
    }

    private void addToOPSIndex(long[] code) {
        if (OPS == null)
            OPS = new HashMap();
        Triple triple = new Triple(code[0], code[1], code[2]);
        if (OPS.containsKey(code[2])) {
            OPS.get(code[2]).add(triple);
        } else {
            ArrayList<Triple> list = new ArrayList();
            list.add(triple);
            OPS.put(code[2], list);
        }
    }

    private ArrayList<QueryStuff.Query> generateQueries() {
        QueryStuff.QueryGenrator queryGenrator = new QueryStuff.QueryGenrator(5, 4, verticies, 4, tripleGraph, 40);
        //ArrayList<QueryStuff.Query> queries = queryGenrator.buildQueries(10);
        ArrayList<QueryStuff.Query> queries = queryGenrator.buildHeavyQueries(10);

        return queries;
    }

    private void putStringTripleInQueries(ArrayList<QueryStuff.Query> queries) {
        for (int i = 0; i < queries.size(); i++) {
            queries.get(i).findStringTriple(reverseDictionary);
            queries.get(i).setQuerySPARQL(reverseDictionary, prefix, verticies);
        }
        //  writeQueriesToFile(queries);
    }

    private void writeQueriesToFile(ArrayList<QueryStuff.Query> queries) {
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

    private void generateQueryGraph(ArrayList<QueryStuff.Query> queries) {
        queryGraph = new QueryStuff.GlobalQueryGraph(queries, 4, POS, SPO, OPS, verticies);
        queryGraph.genrate();


    }


    private HashMap<Long, Integer> codeToPartutpartitionMap;
    private HashMap<Long, Long> borderCodesPartutMap;

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

            QueryStuff.AnnomizedTriple annomizedTriple = queryGraph.getAnnomizedTriples().get(i);
            ArrayList<Triple> fragmentTriples = annomizedTriple.fragment.triples;
            int partutPartition = annomizedTriple.partutAssignedPartition;
            for (int j = 0; j < fragmentTriples.size(); j++) {
             *//*  boolean isAlreadyBorder =  borderCodesPartutMap.get(fragmentTriples.get(j).triples[0]) != null;
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
               *//*
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

        HashMap<Long, ArrayList<String>> ooohAnotherMap = new HashMap();
        Iterator it = tripleToPartutPartitionMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            Integer partNo = (Integer) pair.getValue();

            try {
                long s = Long.valueOf(((String) pair.getKey()).split("p")[0]);
                long p = Long.valueOf(((String) pair.getKey()).split("p")[1]);
                long o = Long.valueOf(((String) pair.getKey()).split("p")[2]);

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
            Long p = (Long) pair.getKey();
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
            QueryStuff.AnnomizedTriple annomizedTriple = queryGraph.getAnnomizedTriples().get(i);
            ArrayList<Triple> fragmentTriples = annomizedTriple.fragment.triples;
            for (int j = 0; j < fragmentTriples.size(); j++) {
                Triple triple = fragmentTriples.get(j);
                QueryStuff.VertexGraph vertex = verticies.get(triple.triples[0]);
                if (vertex.partutPartitionsMap == null)
                    vertex.partutPartitionsMap = new HashMap();
                vertex.partutPartitionsMap.put(annomizedTriple.partutAssignedPartition, annomizedTriple.partutAssignedPartition);
            }

        }

        // now we assign all the triples which does not belong to a fragment

        for (int j = 0; j < verticies.size(); j++) {
            QueryStuff.VertexGraph vertex = verticies.get(j);
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




//this method only trnasofrm sourcefile in quad format to destFile in n3 format
    public void convertNqToN3(String sourceFile, String destFile) {
        int linesWrittenCount = 0;
        File file = new File(sourceFile);
        LineIterator it = null;
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            it = FileUtils.lineIterator(file, "US-ASCII");
           fw = new FileWriter(destFile);
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
                if (count % 10000 == 0) {
                    System.out.println("processing line " + count);
                }
                count++;
                //     if(count == 100000)
                //      break;
                String line = it.nextLine();
                String[] triple;
                if (quad) {
                    triple = getTripleFromQuadLine(line);
                    if (triple == null)
                        continue;
                }

                if (triple == null || triple.length < 3)
                    continue;

                //write the triple to bufferfile
               String tripleStr = triple[0] +" "+triple[1] +" "+triple[2];
                bw.write(tripleStr);
                bw.newLine();
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


    }
*/
}
