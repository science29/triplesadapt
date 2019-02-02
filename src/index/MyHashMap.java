package index;

import util.FileHashMap;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;



//TODO solve the  creation of the filehashmap
public class MyHashMap<K, V> extends HashMap<K, V> implements Serializable {

    private String fileName;
    private FileHashMap<K, V> fileHashMap;
    private FileHashMap<K, String> backupFileHashMap;
    private HashMap<K, V> hashMap;
    private double elemSize = 0;

    private final long fac = 8;
    private final long MAX_SIZE_GB =  fac*1000000000;

    //   private int avgElemSize = 1 ;

    public MyHashMap(String fileName) {
        super();
        try {
            fileHashMap = new FileHashMap(fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hashMap = new HashMap<>();
        this.fileName = fileName;
    }


    private void setSize() {
        try {
            if (hashMap.size() < 10000 || elemSize != 0 || hashMap.size()!=10000 )
                return;

            FileWriter fileWriter = new FileWriter("temp_shjf");
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            int cnt = 0;
            Iterator it = hashMap.entrySet().iterator();
            while (it.hasNext()) {
                if(cnt > 10000)
                    break;
                Map.Entry pair = (Map.Entry)it.next();
                String str = pair.getKey() + " = " + pair.getValue();
                fileWriter.write(str);
                cnt++;
            }
            try {
                bufferedWriter.close();
                fileWriter.close();
            } catch (IOException ex) {

                ex.printStackTrace();

            }
            File file = new File("temp_shjf");
            elemSize = (int)file.length()/10000;
                /*
                System.out.println("Index Size: " + hashMap.size());
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(this);
                oos.close();
                elemSize = baos.size() / hashMap.size();
                System.out.println("Data Size: " + baos.size());*/
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




    @Override
    public boolean containsKey(Object key) {
        if(hashMap.containsKey(key))
            return true;
        if (fileHashMap != null)
            return fileHashMap.containsKey(key);
        return false;
    }


    @Override
    public V get(Object key) {
        if (hashMap != null && hashMap.containsKey(key))
            return hashMap.get(key);
        if (backupFileHashMap != null) {
            String value = (String) backupFileHashMap.get(key);
            ArrayList<Triple> tarr = new ArrayList();
            String elems[] = value.split("algheziAr7");
            for (int i = 0; i < elems.length; i++) {
                String tripleStrA[] = value.split("algheziTr8");
                Triple ttriple = new Triple(Long.valueOf(tripleStrA[0]), Long.valueOf(tripleStrA[1]), Long.valueOf(tripleStrA[2]));
                tarr.add(ttriple);
            }
            return (V) tarr;
        }

        return fileHashMap.get(key);

    }


    @Override
    public V put(K key, V value) {
        setSize();
        if (elemSize * hashMap.size() < MAX_SIZE_GB)
            return hashMap.put(key, value);

        if (value instanceof ArrayList) {
            ArrayList tarr = (ArrayList) value;
            if (tarr.size() > 0 && tarr.get(0) instanceof Triple) {
                String serilaizedArray = "";
                for (int i = 0; i < tarr.size(); i++) {
                    Triple ttriple = (Triple) tarr.get(i);
                    String tripleStr = ttriple.triples[0] + "algheziTr8" + ttriple.triples[1] + "algheziTr8" + ttriple.triples[2];
                    if (i == 0)
                        serilaizedArray = tripleStr;
                    else
                        serilaizedArray = serilaizedArray + "algheziAr7" + tripleStr;
                }
                if (backupFileHashMap == null)
                    try {
                        backupFileHashMap = new FileHashMap("backup" + fileName);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                return (V) backupFileHashMap.put(key, serilaizedArray);
            }
        }
        if (fileHashMap == null)
            try {
                fileHashMap = new FileHashMap(fileName);

            } catch (IOException e) {
                e.printStackTrace();
            }
        return fileHashMap.put(key, value);


    }

}

