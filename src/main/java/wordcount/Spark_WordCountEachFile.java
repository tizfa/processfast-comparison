package wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class Spark_WordCountEachFile {

    private static Pattern pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])");

    static ArrayList<String> readArticles(String filename) {
        ArrayList ret = new ArrayList();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String l;
            while ((l = br.readLine()) != null) {
                // process the line.
                if (l.isEmpty())
                    continue;
                if (l.startsWith("<doc") || l.startsWith("</doc"))
                    continue;

                ret.add(l);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    public static void main(String[] args) {

        if (args.length != 3)
            throw new IllegalArgumentException("Usage: " + Spark_WordCountEachFile.class.getName() + " <inputDir> <outputDir> <numCores>");

        long startTime = System.currentTimeMillis();
        String inputDir = args[0];
        String outputDir = args[1];
        int numCores = Integer.parseInt(args[2]);

        SparkConf conf = new SparkConf().setAppName("Spark word count");
        conf.setMaster("local[" + numCores + "]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> fileList = new ArrayList<String>();
        for (int mainDir = 0; mainDir < 11; mainDir++) {
            for (int subDir = 0; subDir < 100; subDir++) {
                String fname = String.format(inputDir + "/%03d/wiki_%02d", mainDir, subDir);
                if (new File(fname).exists()) {
                    fileList.add(fname);
                }
            }
        }
        JavaRDD<String> textLines = sc.parallelize(fileList);
        HashMap<String, Integer> res = textLines.map(new Function<String, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> call(String filename) throws Exception {
                List<String> articles = readArticles(filename);
                HashMap<String, Integer> mapWords = new HashMap<String, Integer>();
                for (int idx = 0; idx < articles.size(); idx++) {
                    String article = articles.get(idx);
                    String[] a = pattern.split(article);
                    for (int i = 0; i < a.length; i++) {
                        String word = a[i];
                        if (word.isEmpty())
                            continue;
                        String w = word.toLowerCase();
                        if (!mapWords.containsKey(w)) {
                            mapWords.put(w, 1);
                        } else {
                            mapWords.put(w, mapWords.get(w) + 1);
                        }
                    }
                }
                return mapWords;
            }
        }).reduce(new Function2<HashMap<String, Integer>, HashMap<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> call(HashMap<String, Integer> map1, HashMap<String, Integer> map2) throws Exception {
                HashMap<String, Integer> m = new HashMap<String, Integer>();
                m.putAll(map1);
                Iterator<String> keys = map2.keySet().iterator();
                while (keys.hasNext()) {
                    String key = keys.next();
                    int value = map2.get(key);
                    if (m.containsKey(key))
                        m.put(key, m.get(key) + value);
                    else
                        m.put(key, value);
                }
                return m;
            }
        });

        // Write results.
        StringBuilder sb = new StringBuilder();
        Iterator<String> keys = res.keySet().iterator();
        while (keys.hasNext()) {
            String k = keys.next();
            int v = res.get(k);
            sb.append("Word: " + k + " Occurrences: " + v + "\n");
        }
        writeTextFile(outputDir + "/results.txt", sb.toString());

        long endTime = System.currentTimeMillis();
        System.out.println("Done! Execution time: " + (endTime - startTime) + " milliseconds.");
    }


    private static void writeTextFile(String filename, String textToWrite) {
        try {
            new File(filename).getParentFile().mkdirs();
            FileWriter writer = new FileWriter(filename, false);
            BufferedWriter bufferedWriter = new BufferedWriter(writer);
            bufferedWriter.write(textToWrite);
            bufferedWriter.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
