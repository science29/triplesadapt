package start;


import java.util.ArrayList;
import java.util.Scanner;

public class StartUniAdapt {


    public static void main(String[] args) {

        System.out.println();
        System.out.println("Starting UniAdapt..");


        System.out.println("Please enter the a path to .nt data file..");

        Scanner scanner = new Scanner(System.in);
        String fileName = scanner.nextLine();
        InitializeStorage o = new InitializeStorage();
        try{
            o.startGUI();
        }catch (Exception e){
            System.err.println("No GUI started..");
            e.printStackTrace();
        }

        try {
            ArrayList<String> filePaths = new ArrayList<String>();
            //filePaths.add("/home/keg/Desktop/BTC/yago.n3");
            filePaths.add(fileName);
            //filePaths.add("/Users/apple/IdeaProjects/LUBM temp/data/University1_.nt");
            try {
                o.process(filePaths, false);
            }catch (Exception e){
                e.printStackTrace();
            }

            o.printTransporterSummary();
            o.listenToQuery();

        }catch (Exception e){
            e.printStackTrace();
            o.finish();
        }


    }







}
