package optimizer.GUI;

import QueryStuff.QueryStreamGenerator;
import index.IndexesPool;
import index.MyHashMap;
import optimizer.EngineRotater2;
import triple.Triple;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

public class OptimizerGUI {
    private JPanel panel1;
    private JLabel border;
    private JLabel total;
    private JLabel action;
    private JButton startGeneratingQueriesStreamButton;
    private JButton decrease_max_length;
    private JButton increase_max_length;
    private JButton increase_period;
    private JButton decrease_peroid;
    private JButton increase_quality;
    private JButton decrease_quality;
    private JLabel query_status;
    private JLabel max_length;
    private JLabel period;
    private JLabel quality;
    private JButton optimizerStart;

    private JLabel SPo;
    private JLabel OPs;
    private JLabel PSo;
    private JLabel SOp;
    private JLabel OSp;
    private JLabel POs;
    private JButton genQBatchButton;


    public static OptimizerGUI createForm(QueryStreamGenerator queryStreamGenerator , EngineRotater2 optimizer , boolean GUISupport) {
        OptimizerGUI t = new OptimizerGUI(queryStreamGenerator , optimizer);
        if(!GUISupport){
            return t;
        }
        JFrame frame = new JFrame("OptimizerGUI");
       frame.setContentPane(t.panel1);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
        return t;
    }


    public OptimizerGUI(QueryStreamGenerator queryStreamGenerator , EngineRotater2 optimizer){
        setListener(queryStreamGenerator, optimizer);
    }



    private void setListener(QueryStreamGenerator queryStreamGenerator , EngineRotater2 optimizer){

        optimizerStart.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                optimizer.work();
            }
        });



        genQBatchButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if(!queryStreamGenerator.working) {
                    queryStreamGenerator.setModeBatch();
                    queryStreamGenerator.startThread();
                    genQBatchButton.setText("generating queries");
                }
                else {
                    queryStreamGenerator.stopGenearaingThread();
                    startGeneratingQueriesStreamButton.setText("gen. q. batch");
                }
            }
        });


        startGeneratingQueriesStreamButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if(!queryStreamGenerator.working) {
                    queryStreamGenerator.setModeStream();
                    queryStreamGenerator.startThread();
                    startGeneratingQueriesStreamButton.setText("stop generating queries");
                }
                else {
                    queryStreamGenerator.stopGenearaingThread();
                    startGeneratingQueriesStreamButton.setText("generate q. stream");
                }
            }
        });

        setStreamVal(queryStreamGenerator);
        increase_max_length.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                queryStreamGenerator.increaseMaxLength(true);
                setStreamVal(queryStreamGenerator);
            }
        });

        decrease_max_length.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                queryStreamGenerator.increaseMaxLength(false);
                setStreamVal(queryStreamGenerator);
            }
        });

        increase_period.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                queryStreamGenerator.increasePeroid(true);
            }
        });

        decrease_peroid.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                queryStreamGenerator.increasePeroid(false);
            }
        });

        increase_quality.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                queryStreamGenerator.increaseQuality(true);
            }
        });

        decrease_quality.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                queryStreamGenerator.increaseQuality(false);
            }
        });

    }



    public void setStreamVal(QueryStreamGenerator queryStreamGenerator) {

        quality.setText("quality: "+queryStreamGenerator.getCurrentQueryQuality()+"");
        period.setText("current period: "+queryStreamGenerator.getCurrentPeroid()+"");
        max_length.setText("max length: "+queryStreamGenerator.getMaxLength()+"");

    }



    public void setIndexes(IndexesPool indexesPool){
        try {
            MyHashMap<Integer, ArrayList<Triple>> indexx = indexesPool.getIndex(IndexesPool.SPo);
            if (indexx != null) {
                SPo.setText("SPo:" + indexx.size());
            }

            if (indexesPool.getIndex(IndexesPool.OPs) != null)
                OPs.setText("OPs:" + indexesPool.getIndex(IndexesPool.OPs).size());

            if (indexesPool.getIndex(IndexesPool.PSo) != null)
                PSo.setText("PSo:" + indexesPool.getIndex(IndexesPool.PSo).size());

            if (indexesPool.getIndex(IndexesPool.SOp) != null)
                SOp.setText("SOp:" + indexesPool.getIndex(IndexesPool.SOp).size());

            if (indexesPool.getIndex(IndexesPool.OSp) != null)
                OSp.setText("OSp:" + indexesPool.getIndex(IndexesPool.OSp).size());

            if (indexesPool.getIndex(IndexesPool.POs) != null)
                POs.setText("POs:" + indexesPool.getIndex(IndexesPool.POs).size());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    void setReplication(IndexesPool indexesPool){
        if(indexesPool.getIndex(IndexesPool.SPo_r) != null)
            border.setText("Border replication:"+indexesPool.getIndex(IndexesPool.SPo_r).size());
    }

    public void setAction(String actionText){
        action.setText(actionText);

    }

}
