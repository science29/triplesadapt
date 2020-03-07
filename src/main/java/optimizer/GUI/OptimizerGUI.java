package optimizer.GUI;

import QueryStuff.QueryStreamGenerator;
import index.IndexesPool;
import optimizer.Optimizer2;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

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


    public static OptimizerGUI createForm(QueryStreamGenerator queryStreamGenerator , Optimizer2 optimizer) {
        JFrame frame = new JFrame("OptimizerGUI");
        OptimizerGUI t = new OptimizerGUI(queryStreamGenerator , optimizer);
        frame.setContentPane(t.panel1);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
        return t;
    }


    public OptimizerGUI(QueryStreamGenerator queryStreamGenerator , Optimizer2 optimizer){
        setListener(queryStreamGenerator, optimizer);
    }



    private void setListener(QueryStreamGenerator queryStreamGenerator , Optimizer2 optimizer){

        optimizerStart.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                optimizer.work();
            }
        });

        startGeneratingQueriesStreamButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if(!queryStreamGenerator.working) {
                    queryStreamGenerator.startThread();
                    startGeneratingQueriesStreamButton.setText("stop generating queries");
                }
                else {
                    queryStreamGenerator.stopGenearaingThread();
                    startGeneratingQueriesStreamButton.setText("start generating queries");
                }
            }
        });

        setStreamVal(queryStreamGenerator);
        increase_max_length.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                queryStreamGenerator.increaseMaxLength(true);
            }
        });

        decrease_max_length.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                queryStreamGenerator.increaseMaxLength(false);
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



    void setIndexes(IndexesPool indexesPool){
        if(indexesPool.getIndex(IndexesPool.SPo) != null )
            SPo.setText("SPo:"+indexesPool.getIndex(IndexesPool.SPo).size());

        if(indexesPool.getIndex(IndexesPool.OPs) != null )
            OPs.setText("OPs:"+indexesPool.getIndex(IndexesPool.OPs).size());

        if(indexesPool.getIndex(IndexesPool.PSo) != null )
            PSo.setText("PSo:"+indexesPool.getIndex(IndexesPool.PSo).size());

        if(indexesPool.getIndex(IndexesPool.SOp) != null )
            SOp.setText("SOp:"+indexesPool.getIndex(IndexesPool.SOp).size());

        if(indexesPool.getIndex(IndexesPool.OSp) != null )
            OSp.setText("OSp:"+indexesPool.getIndex(IndexesPool.OSp).size());

        if(indexesPool.getIndex(IndexesPool.POs) != null )
            POs.setText("POs:"+indexesPool.getIndex(IndexesPool.POs).size());
    }

    void setReplication(IndexesPool indexesPool){
        if(indexesPool.getIndex(IndexesPool.SPo_r) != null)
            border.setText("Border replication:"+indexesPool.getIndex(IndexesPool.SPo_r).size());
    }

    public void setAction(String actionText){
        action.setText(actionText);

    }

}
