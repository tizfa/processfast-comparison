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

import it.cnr.isti.hlt.processfast.core.ProcessfastRuntime;
import it.cnr.isti.hlt.processfast.core.TaskContext;
import it.cnr.isti.hlt.processfast.data.CollectionDataSourceIteratorProvider;
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;
import it.cnr.isti.hlt.processfast.data.RamDictionary;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast_mt.core.MTProcessfastRuntime;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;


class ProcessFast_WordCountEachFile {
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
            HashMap<String, Integer> mw = pd.withPartitionSize(150)
                    .map((tdc, item) -> {
                        List<String> articles = readArticles(item.getV2());
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
                    })
                    .reduce((tdc, map1, map2) -> {
                        Iterator<String> keys = map2.keySet().iterator();
                        while (keys.hasNext()) {
                            String key = keys.next();
                            int value = map2.get(key);
                            if (map1.containsKey(key))
                                map1.put(key, map1.get(key) + value);
                            else
                                map1.put(key, value);
                        }
                        return map1;
                    });

            // Write results.
            StringBuilder sb = new StringBuilder();
            Iterator<String> keys = mw.keySet().iterator();
            while (keys.hasNext()) {
                String k = keys.next();
                int v = mw.get(k);
                sb.append("Word: " + k + " Occurrences: " + v + "\n");
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
            throw new IllegalArgumentException("Usage: " + ProcessFast_WordCountEachFile.class.getName() + " <inputDir> <outputDir> <numCores>");
        long startTime = System.currentTimeMillis();
        MTProcessfastRuntime runtime = new MTProcessfastRuntime();
        runtime.setNumThreadsForDataParallelism(Integer.parseInt(args[2]));
        runProgram(runtime, args[0], args[1]);
        long endTime = System.currentTimeMillis();
        System.out.println("Done! Execution time: " + (endTime - startTime) + " milliseconds.");
    }
}
