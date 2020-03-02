package optimizer.GUI;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;


public class StarterGUI {
    private JButton makeIntialMetisFileButton;
    private JButton doMetisButton;
    private JPanel panel;
    private JLabel status;
    private JButton readMetisOut;

    public StarterGUI(ButtonsListener listener) {
        makeIntialMetisFileButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                listener.makeIntialMetisFile();
            }
        });
        readMetisOut.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                listener.readOutMetisFile();
            }
        });
    }

    public static StarterGUI getInstance(ButtonsListener listener) {
        JFrame frame = new JFrame("StarterGUI");
        StarterGUI starter = new StarterGUI(listener);
        frame.setContentPane(starter.panel);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
        return starter;
    }

    public void writeAction(String action) {
        status.setText(action);
    }


    public interface ButtonsListener{
        void makeIntialMetisFile();

        void readOutMetisFile();
    }

}
