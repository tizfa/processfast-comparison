package wordcount;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class Spark_WordCountEachLine {

    private static Pattern pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])");

    public static void main(String[] args) {

        if (args.length != 3)
            throw new IllegalArgumentException("Usage: " + Spark_WordCountEachLine.class.getName() + " <inputDir> <outputDir> <numCores>");

        long startTime = System.currentTimeMillis();

        String inputDir = args[0];
        String outputDir = args[1];
        int numCores = Integer.parseInt(args[2]);

        SparkConf conf = new SparkConf().setAppName("Spark word count");
        conf.setMaster("local[" + numCores + "]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textLines = sc.textFile(inputDir + "/*");
        List<scala.Tuple2<String, Integer>> res = textLines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterable<scala.Tuple2<String, Integer>> call(String line) throws Exception {
                if (line.isEmpty())
                    return new ArrayList<scala.Tuple2<String, Integer>>();
                if (line.startsWith("<doc") || line.startsWith("</doc"))
                    return new ArrayList<scala.Tuple2<String, Integer>>();

                ArrayList<scala.Tuple2<String, Integer>> listValues = new ArrayList<scala.Tuple2<String, Integer>>();
                String[] a = pattern.split(line);
                for (int i = 0; i < a.length; i++) {
                    String w = a[i];
                    if (w.isEmpty())
                        continue;
                    w = w.toLowerCase();
                    scala.Tuple2<String, Integer> t = new scala.Tuple2<String, Integer>(w, 1);
                    listValues.add(t);
                }
                return listValues;
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer val1, Integer val2) throws Exception {
                return val1 + val2;
            }
        }).collect();

        // Write results.
        StringBuilder sb = new StringBuilder();
        Iterator<scala.Tuple2<String, Integer>> keys = res.iterator();
        while (keys.hasNext()) {
            scala.Tuple2<String, Integer> tuple = keys.next();
            String k = tuple._1();
            int v = tuple._2();
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
