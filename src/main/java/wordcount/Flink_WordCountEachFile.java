/*
 *
 * ****************
 * Copyright 2015 Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ******************
 */

package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public class Flink_WordCountEachFile {

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
            throw new IllegalArgumentException("Usage: " + Flink_WordCountEachFile.class.getName() + " <inputFile> <outputDir> <numCores>");

        long startTime = System.currentTimeMillis();

        String inputDir = args[0];
        String outputDir = args[1];
        int numCores = Integer.parseInt(args[2]);

        File f = new File(inputDir);
        if (!f.exists())
            throw new IllegalArgumentException("Can not find inputfile " + f.getName());

        List<String> fileList = new ArrayList<String>();
        for (int mainDir = 0; mainDir < 11; mainDir++) {
            for (int subDir = 0; subDir < 100; subDir++) {
                String fname = String.format(inputDir + "/%03d/wiki_%02d", mainDir, subDir);
                if (new File(fname).exists()) {
                    fileList.add(fname);
                }
            }
        }

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(numCores);

        // create a configuration object
        Configuration parameters = new Configuration();

        // set the recursive enumeration parameter
        parameters.setBoolean("recursive.file.enumeration", true);

        env.setParallelism(numCores);
        DataSet<String> files = env.fromCollection(fileList);

        try {
            HashMap<String, Integer> res = files.map(new MapFunction<String, HashMap<String, Integer>>() {
                @Override
                public HashMap<String, Integer> map(String filename) throws Exception {
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
            })
                    .setParallelism(numCores)
                    .reduce(new ReduceFunction<HashMap<String, Integer>>() {
                        @Override
                        public HashMap<String, Integer> reduce(HashMap<String, Integer> map1, HashMap<String, Integer> map2) throws Exception {
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
                    })
                    .setParallelism(numCores)
                    .collect().get(0);


            // Write results.
            StringBuilder sb = new StringBuilder();
            Iterator<String> keys = res.keySet().iterator();
            while (keys.hasNext()) {
                String k = keys.next();
                int v = res.get(k);
                sb.append("Word: " + k + " Occurrences: " + v + "\n");
            }
            writeTextFile(outputDir + "/results.txt", sb.toString());


            env.execute();
        } catch (Exception e) {
            throw new RuntimeException("Computing words occurrence", e);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Execution time: " + (endTime - startTime) + " milliseconds.");
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

    // User-defined functions
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

}
