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

import it.cnr.isti.hlt.processfast.connector.*;
import it.cnr.isti.hlt.processfast.core.ProcessfastRuntime;
import it.cnr.isti.hlt.processfast.core.TaskContext;
import it.cnr.isti.hlt.processfast.core.TaskSet;
import it.cnr.isti.hlt.processfast.data.CollectionDataSourceIteratorProvider;
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast_mt.core.MTRuntime;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;


class ProcessFast_WordCountFileStreamingPD {
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

    static TaskSet createMainTasksSet(ProcessfastRuntime runtime, int numWorkers, String inputDir, String outputDir, int maxFilesToRead) {
        // Create main tasks set.
        TaskSet ts = runtime.createTaskSet();
        ts.getDataDictionary().put("INPUT_DIR", inputDir);
        ts.getDataDictionary().put("OUTPUT_DIR", outputDir);

        ts.createConnector("DISTRIBUTOR", ConnectorType.LOAD_BALANCING_QUEUE, maxFilesToRead);
        ts.createBarrier("WORKER_BARRIER");

        // Distributor.
        ts.task((TaskContext tc) -> {
            ConnectorWriter dist = tc.getConnectorManager().getConnectorWriter("DISTRIBUTOR");

            List<String> fileList = new ArrayList<String>();
            for (int mainDir = 0; mainDir < 11; mainDir++) {
                for (int subDir = 0; subDir < 100; subDir++) {
                    String fname = String.format(inputDir + "/%03d/wiki_%02d", mainDir, subDir);
                    if (new File(fname).exists()) {
                        fileList.add(fname);
                    }
                }
            }

            Iterator<String> files = fileList.iterator();
            while (files.hasNext()) {
                String fname = files.next();
                ArrayList<String> articles = readArticles(fname);
                dist.putValue(articles);
            }

            dist.signalEndOfStream();
        }).withConnectors(wci -> {
            wci.getConnectorManager().attachTaskToConnector(wci.getTaskName(), "DISTRIBUTOR", ConnectorCapability.WRITE);
        }).onVirtualMachine(vmi -> "vm");


        // Define the worker process.
        ts.task((TaskContext tc) -> {

            ConnectorReader dist = tc.getConnectorManager().getConnectorReader("DISTRIBUTOR");
            int maxFilesBuffered = maxFilesToRead;
            ConnectorMessage cm = null;
            ArrayList<ArrayList<String>> articlesBuffered = new ArrayList<ArrayList<String>>();
            HashMap<String, Integer> mapWordsGlobal = new HashMap<String, Integer>();
            while (true) {
                articlesBuffered.clear();
                while ((cm = dist.getValue()) != null && articlesBuffered.size() < maxFilesBuffered) {
                    articlesBuffered.add((ArrayList<String>) cm.getPayload());
                }
                if (articlesBuffered.size() == 0)
                    break;

                PartitionableDataset<Pair<Integer, ArrayList<String>>> pd = tc.createPartitionableDataset(new CollectionDataSourceIteratorProvider<ArrayList<String>>(articlesBuffered));
                HashMap<String, Integer> mw = pd.map((tdc, item) -> {
                    List<String> articles = item.getV2();
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
                        });


                Iterator<String> keys = mw.keySet().iterator();
                while (keys.hasNext()) {
                    String k = keys.next();
                    int v = mw.get(k);
                    if (mapWordsGlobal.containsKey(k))
                        mapWordsGlobal.put(k, mapWordsGlobal.get(k) + v);
                    else
                        mapWordsGlobal.put(k, v);
                }

            }

            // Write results.
            StringBuilder sb = new StringBuilder();
            Iterator<String> keys = mapWordsGlobal.keySet().iterator();
            while (keys.hasNext()) {
                String k = keys.next();
                int v = mapWordsGlobal.get(k);
                sb.append("Word: " + k + " Occurrences: " + v + "\n");
            }
            writeTextFile(outputDir + "\\results.txt", sb.toString());
        }).withNumInstances(1, 1).withConnectors(wci -> {
            wci.getConnectorManager().attachTaskToConnector(wci.getTaskName(), "DISTRIBUTOR", ConnectorCapability.READ);
        }).onVirtualMachine(
                vmi -> "vm"
        );

        return ts;
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
        if (args.length != 4)
            throw new IllegalArgumentException("Usage: " + ProcessFast_WordCountFileStreamingPD.class.getName() + " <inputDir> <outputDir> <numCores> <maxFilesToRead>");

        long startTime = System.currentTimeMillis();
        MTRuntime runtime = new MTRuntime();
        runtime.setNumThreadsForDataParallelism(Integer.parseInt(args[2]));
        TaskSet ts = createMainTasksSet(runtime, Integer.parseInt(args[2]), args[0], args[1], Integer.parseInt(args[3]));
        runtime.run(ts);
        long endTime = System.currentTimeMillis();
        System.out.println("Done! Execution time: " + (endTime - startTime) + " milliseconds.");
    }
}
