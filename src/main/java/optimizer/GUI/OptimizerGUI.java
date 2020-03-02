package optimizer.GUI;

import index.IndexesPool;

import javax.swing.*;

public class OptimizerGUI {
    private JPanel panel1;
    private JLabel border;
    private JLabel total;
    private JLabel action;

    private JLabel SPo;
    private JLabel OPs;
    private JLabel PSo;
    private JLabel SOp;
    private JLabel OSp;
    private JLabel POs;


    public static OptimizerGUI createForm() {
        JFrame frame = new JFrame("OptimizerGUI");
        OptimizerGUI t = new OptimizerGUI();
        frame.setContentPane(t.panel1);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
        return t;
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
