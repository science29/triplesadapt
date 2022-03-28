package start;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

public class DataDistiributer {


    private static boolean quad = false;

    public void buildTriplesFromFile(ArrayList<String> filePathList) {


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
                int count = 0;
                ArrayList<String> header = new ArrayList<>();
                HashMap<String , String> prefix = new HashMap<>();
                Integer errCount = 0;
                Integer errSolved = 0;
                while (it.hasNext()) {
                    count++;
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
                        triple = MainUinAdapt.getTripleFromQuadLine(line , header , prefix);
                        if (triple == null)
                            continue;
                    } else {
                        triple = MainUinAdapt.getTripleFromTripleLine(line, errCount, errSolved , header , prefix);
                    }
                    if (triple == null || triple.length < 3) {
                        continue;
                    }
                    if (triple[0].equals(triple[1]))
                        continue;

                }



            } finally {
                LineIterator.closeQuietly(it);
            }

        }

    }
}
