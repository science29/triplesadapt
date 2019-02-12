package index;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import triple.Triple;
import util.FileHashMap;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentMap;


//TODO solve the  creation of the filehashmap
public class MyHashMap<K, V> extends HashMap<K, V> implements Serializable {

    private String fileName;
    ConcurrentMap<K, V> fastFileMap;
    private HTreeMap<K, String> fastFileMapString;
    DB fileDb;
    private FileHashMap<K, V> fileHashMap;
    private FileHashMap<K, String> backupFileHashMap;

    private HashMap<K, V> hashMap;
    private double elemSize = 0;

    private double maxAllowedRamSize ;
    private final long fac = 16;
    private final long MAX_SIZE_GB =  fac*1000000000;
    private long n = 1;
    private final long GB =  n*1000000000;

    public final IndexType indexType ;

    public static final int [] noKeyType = {0,0,0};

    public boolean onlyDiskGet = false;
    public boolean diskMemPut = false;

    private final String HOME_DIR = "/home/ahmed/";



    //   private int avgElemSize = 1 ;
    public MyHashMap(String fileName){
        super();
        try {
            fileHashMap = new FileHashMap(HOME_DIR+fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hashMap = new HashMap();
        this.fileName = fileName;
        this.indexType = new IndexType();
        maxAllowedRamSize = MAX_SIZE_GB;
    }

    public MyHashMap(String fileName,double memSizeGB){
        super();
        try {
            fileHashMap = new FileHashMap(HOME_DIR+fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hashMap = new HashMap();
        this.fileName = fileName;
        this.indexType = new IndexType();
        this.maxAllowedRamSize = GB * memSizeGB;
    }

    public MyHashMap(String fileName, IndexType indexType) {
        super();
        try {
            fileHashMap = new FileHashMap(HOME_DIR+fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hashMap = new HashMap();
        this.fileName = fileName;
        this.indexType = indexType;
        maxAllowedRamSize = MAX_SIZE_GB;
    }


    private void setSize() {
        try {
            if (hashMap.size() != 40000 || elemSize != 0 )
                return;

            FileWriter fileWriter = new FileWriter(HOME_DIR+"temp_shjf");
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            int cnt = 0;
            Iterator it = hashMap.entrySet().iterator();
            while (it.hasNext()) {
                if(cnt > 40000)
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
            File file = new File(HOME_DIR+"temp_shjf");
            if(file.length() < 100000000)
                file.length();
            elemSize = (int)file.length()/40000;
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
        //todo fix this
        if(this.onlyDiskGet)
            return fileHashMap.get(key);

        if (hashMap != null && hashMap.containsKey(key))
            return hashMap.get(key);

        if(fastFileMapString != null) {
            String value = fastFileMapString.get(key);
            if (value == null)
                return null;
            ArrayList<Triple> tarr = new ArrayList();
            String elems[] = value.split("algheziAr7");
            for (int i = 0; i < elems.length; i++) {
                String tripleStrA[] = value.split("algheziTr8");
                Triple ttriple = new Triple(Long.valueOf(tripleStrA[0]), Long.valueOf(tripleStrA[1]), Long.valueOf(tripleStrA[2]));
                tarr.add(ttriple);
            }
            return (V) tarr;
        }

        if (backupFileHashMap != null) {
            String value = (String) backupFileHashMap.get(key);
            if(value == null)
                return null;
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
        //todo fix this
        if(diskMemPut)
            fileHashMap.put(key, value);
        if (elemSize * hashMap.size() < maxAllowedRamSize)
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
                if(fastFileMapString == null){
                    File file = new File(HOME_DIR + fileName + "db.db");
                    if(file.exists())
                        file.delete();
                    org.mapdb.Serializer<K> keyType = findTypeKey(key);
                    org.mapdb.Serializer<String> valType =  Serializer.STRING;
                    if(keyType != null && valType != null) {
                        fileDb = DBMaker
                                .fileDB(HOME_DIR + fileName + "db.db")
                                .fileMmapEnable()
                                .make();
                        fastFileMapString = fileDb.hashMap("map", keyType, valType)
                                .createOrOpen();
                    }
                }
                if(fastFileMapString != null) {
                    return (V) fastFileMapString.put(key, serilaizedArray);
                }

                if (backupFileHashMap == null)
                    try {
                        backupFileHashMap = new FileHashMap(HOME_DIR+"backup" + fileName);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                return (V) backupFileHashMap.put(key, serilaizedArray);
            }
        }
        if (fileHashMap == null)
            try {
                fileHashMap = new FileHashMap(HOME_DIR+fileName);

            } catch (IOException e) {
                e.printStackTrace();
            }
        return fileHashMap.put(key, value);


    }

    private Serializer<K> findTypeKey(K obj) {
        if(obj instanceof  Long)
            return (Serializer<K>) Serializer.LONG;
        if(obj instanceof  Integer)
            return (Serializer<K>) Serializer.INTEGER;
        if(obj instanceof  String)
            return (Serializer<K>) Serializer.STRING;

        return null;

    }

    private Serializer<V> findTypeVal(V obj) {
        if(obj instanceof  Long)
            return (Serializer<V>) Serializer.LONG;
        if(obj instanceof  Integer)
            return (Serializer<V>) Serializer.INTEGER;
        if(obj instanceof  String)
            return (Serializer<V>) Serializer.STRING;

        return null;

    }


    @Override
    public Set<Entry<K,V>> entrySet() {
        if(fastFileMapString != null){
            fastFileMap = (ConcurrentMap<K, V>) fastFileMapString;
            return fastFileMap.entrySet();
        }
        return hashMap.entrySet();
    }

    public Set<Entry<K,V>> fileEntrySet(){
        return fileHashMap.entrySet();
    }



    @Override
    public int size() {
        return hashMap.size()+fileHashMap.size();
    }

    public double getSizeGB(){
        return  elemSize*size()/1000000000;
    }


    public String getFileName() {
        return fileName;
    }

    public double getMemorySize() {
        if(hashMap== null)
            return 0;
        double s =  ((hashMap.size()/1000000)*elemSize)/1000;
        return s;
    }
}

