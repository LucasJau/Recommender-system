package my_simple_apps.app;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Outlook extends JDialog {
    private JPanel contentPane;
    private JButton button1;
    private JButton button2;
    private JButton button3;
    private JButton button4;
    private JButton button5;
    private JButton 没有看过的换几部Button;
    private JButton 选好了给我推荐吧Button;

    public void putOutlook(Outlook ol) {
        setContentPane(contentPane);
        setModal(true);
        final String fileName = "D:/ml-1m/freshman.dat";
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        App app = new App();
        final List<Tuple2<Integer, String>> movieNames = app.getMovieName(sc);
        final List<String> fmInfo = new ArrayList<String>();
        button1.setText(movieNames.get(0)._2);
        button2.setText(movieNames.get(1)._2);
        button3.setText(movieNames.get(2)._2);
        button4.setText(movieNames.get(3)._2);
        button5.setText(movieNames.get(4)._2);
        setDefaultCloseOperation(DISPOSE_ON_CLOSE);
        final Outlook olf = ol;
        button1.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                Integer rate = onClick(e, olf, movieNames.get(0)._1);
            }
        });

        button2.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                Integer rate = onClick(e, olf, movieNames.get(1)._1);
            }
        });
        button3.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                Integer rate = onClick(e, olf, movieNames.get(2)._1);
            }
        });
        button4.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                Integer rate = onClick(e, olf, movieNames.get(3)._1);
            }
        });
        button5.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                Integer rate = onClick(e, olf, movieNames.get(4)._1);
            }
        });
        没有看过的换几部Button.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                olf.putOutlook(olf);
                olf.pack();
                olf.setVisible(true);
            }
        });
        选好了给我推荐吧Button.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                App app1 = new App();
                String str = "推荐结果为：\n";
                List<String> strlist = app1.run();
                for(int i = 0; i < 5; i++) {
                    str = str + strlist.get(i) + "\n";
                }
                JOptionPane.showMessageDialog(null, str);
            }
        });
        sc.stop();
    }

    private int onClick(ActionEvent e, Outlook ol, Integer id)
    {
        String fileName = "D:/ml-1m/freshman.dat";
        Integer rate = Integer.valueOf(JOptionPane.showInputDialog("请输入您对该电影的评分（1-5）："));
        String str = "0::"+id.toString()+"::"+rate.toString()+"\n";
        FileWriter writer = null;
        try {
            writer = new FileWriter(fileName, true);
            writer.write(str);
            writer.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        ol.putOutlook(ol);
        this.setVisible(false);
        ol.pack();
        ol.setVisible(true);
        //System.exit(0);
        return rate;
    }

    public static void init()
    {
        String fileName = "D:/ml-1m/freshman.dat";
        FileWriter writer = null;
        try {
            writer = new FileWriter(fileName);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    public static void main(String[] args) {
        init();
        Outlook dialog = new Outlook();
        dialog.putOutlook(dialog);
        dialog.pack();
        dialog.setVisible(true);
    }
}
