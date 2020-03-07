package start;

import javax.swing.*;

public class tempp {
    private JButton button1;
    private JPanel panel1;

    public static tempp createForm() {
        JFrame frame = new JFrame("OptimizerGUI");
        tempp t = new tempp();
        frame.setContentPane(t.panel1);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
        return t;
    }

}
