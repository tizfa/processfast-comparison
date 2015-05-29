/*
 * *****************
 *  Copyright 2015 Tiziano Fagni (tiziano.fagni@isti.cnr.it)
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
 * *******************
 */

package wordcount;

import it.cnr.isti.hlt.processfast.core.ProcessfastRuntime;
import it.cnr.isti.hlt.processfast.core.TaskContext;
import it.cnr.isti.hlt.processfast.core.TaskDataContext;
import it.cnr.isti.hlt.processfast.data.CollectionDataSourceIteratorProvider;
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;
import it.cnr.isti.hlt.processfast.data.RamDictionary;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast_mt.core.MTRuntime;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;


class ProcessFast_WordCountEachFile2 {
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

    static void runProgram(ProcessfastRuntime runtime, String inputDir, String outputDir) {

        runtime.run(new RamDictionary(), (TaskContext tc) -> {
            List<String> fileList = new ArrayList<String>();
            for (int mainDir = 0; mainDir < 11; mainDir++) {
                for (int subDir = 0; subDir < 100; subDir++) {
                    String fname = String.format(inputDir + "/%03d/wiki_%02d", mainDir, subDir);
                    if (new File(fname).exists()) {
                        fileList.add(fname);
                    }
                }
            }


            PartitionableDataset<Pair<Integer, String>> pd = tc.createPartitionableDataset(new CollectionDataSourceIteratorProvider<String>(fileList));
            List<Pair<String, Integer>> mw = pd.withPartitionSize(100)
                    .mapPairFlat((TaskDataContext tdc, Pair<Integer, String> item) -> {
                        List<String> articles = readArticles(item.getV2());
                        ArrayList<Pair<String, Integer>> values = new ArrayList<Pair<String, Integer>>(1000000);
                        for (int idx = 0; idx < articles.size(); idx++) {
                            String article = articles.get(idx);
                            String[] a = pattern.split(article);
                            for (int i = 0; i < a.length; i++) {
                                String word = a[i];
                                if (word.isEmpty())
                                    continue;
                                String w = word.toLowerCase();
                                values.add(new Pair<String, Integer>(w, 1));
                            }
                        }
                        return values.iterator();
                    })
                    .reduceByKey((tdc, v1, v2) -> {
                        return v1 + v2;
                    }).collect();

            // Write results.
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < mw.size(); i++) {
                Pair<String, Integer> val = mw.get(i);
                sb.append("Word: " + val.getV1() + " Occurrences: " + val.getV2() + "\n");
            }
            writeTextFile(outputDir + "\\results.txt", sb.toString());

        });

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

    public static void main(String[] args) {
        if (args.length != 3)
            throw new IllegalArgumentException("Usage: " + ProcessFast_WordCountEachFile2.class.getName() + " <inputDir> <outputDir> <numCores>");
        long startTime = System.currentTimeMillis();
        MTRuntime runtime = new MTRuntime();
        runtime.setNumThreadsForDataParallelism(Integer.parseInt(args[2]));
        runProgram(runtime, args[0], args[1]);
        long endTime = System.currentTimeMillis();
        System.out.println("Done! Execution time: " + (endTime - startTime) + " milliseconds.");
    }
}
