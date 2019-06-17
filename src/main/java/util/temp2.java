package util;

import java.util.Random;

public class temp2 {

    public static void work(){



        int memoryCost = 100;
        int DiskCost = 10000;
        int networkCost = 60000;

        int memorySize = 130;
        int diskSize = 300;


        int forReplicationSize = 40;
        int toReplicationSize = 80;
        int maxQueryLength = 6;

        double devitionFactor = 0.25;
        double fixingFactor = 0.75;
        double ok = 0;
        double dataSetLength = 30;
        double worklodDeviation = diskSize/6;
        Random qlRandm = new Random();
        Random memRandm = new Random();
        Random remoteRandom = new Random();
        Random replicationRandom = new Random();
        double exeTime = 0;
        double exeTimeW = 0;
        double extTimeUSRan = 0;
        for(int i =0 ; i< 2500 ; i++){
            double ql = qlRandm.nextInt(maxQueryLength - 2)+2;
            boolean local  = remoteRandom.nextDouble() > ql/dataSetLength;
         //   if(!local)
           //     local = Math.abs(replicationRandom.nextGaussian() * toReplicationSize/2)  < toReplicationSize/2;
            double rt = memRandm.nextGaussian() ;
            double PM = rt * worklodDeviation  ;
            boolean memoryAccess = false;
            boolean memoryAccessUnfirm = false;
            if(Math.abs(PM) + diskSize/devitionFactor <= memorySize/2)
                memoryAccess = true;
            else{
                if(!local) {
                    local = replicationRandom.nextGaussian() * toReplicationSize/2 + diskSize/devitionFactor < forReplicationSize ;
                }
            }

            memoryAccessUnfirm = (Math.abs(memRandm.nextInt(diskSize)) <= memorySize);



            int M = memoryCost;
            int D = DiskCost;
            int N = networkCost;
            int Dwa = D;
            int DusRan = D;
            if(local) {
                N = 0;
                //Dwa = 0;
            }
            exeTimeW = exeTimeW +ql * M + Dwa + N;
            if(memoryAccessUnfirm) {
                DusRan = 0;
                N = 0;
            }
                extTimeUSRan = extTimeUSRan +ql * M + DusRan + N;
            if(memoryAccess){
                N = 0 ;
                D = 0;
            }else {
                if (local)
                    N = 0;
                else
                    D = 0;
            }
           // N = 0;
            exeTime =  exeTime+ql * M + D + N;
            ok = ok+rt;
            System.out.println(i+" "+exeTime/100000 +" "+exeTimeW/100000+" "+extTimeUSRan/100000 ); //+" devitionFactor="+ devitionFactor);
         //   if(memoryAccess)
           //     ok++;


            if(i > 10) {
                devitionFactor = devitionFactor+fixingFactor;
                fixingFactor = fixingFactor * 1.2;
            }
        }
        System.out.println("ok="+ok+ ", the rate: "+ok/2500.0);
    }
}
