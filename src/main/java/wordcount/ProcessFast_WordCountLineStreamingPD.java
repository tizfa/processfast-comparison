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
import it.cnr.isti.hlt.processfast.data.RecursiveFileLineIteratorProvider;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast_mt.core.MTRuntime;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;


class ProcessFast_WordCountLineStreamingPD {


    static TaskSet createMainTasksSet(ProcessfastRuntime runtime, int numWorkers, String inputDir, String outputDir, int maxLinesToRead) {
        // Create main tasks set.
        TaskSet ts = runtime.createTaskSet();
        ts.getDataDictionary().put("INPUT_DIR", inputDir);
        ts.getDataDictionary().put("OUTPUT_DIR", outputDir);

        ts.createConnector("DISTRIBUTOR", ConnectorType.LOAD_BALANCING_QUEUE, maxLinesToRead);
        ts.createBarrier("WORKER_BARRIER");

        // Distributor.
        ts.task((TaskContext tc) -> {
            ConnectorWriter dist = tc.getConnectorManager().getConnectorWriter("DISTRIBUTOR");
            RecursiveFileLineIteratorProvider provider = new RecursiveFileLineIteratorProvider(inputDir, "");
            Iterator<String> lines = provider.iterator();

            while (lines.hasNext()) {
                String line = lines.next();
                if (line.isEmpty())
                    continue;
                if (line.startsWith("<doc") || line.startsWith("</doc"))
                    continue;
                dist.putValue(line);
            }

            dist.signalEndOfStream();
        }).withConnectors(wci -> {
            wci.getConnectorManager().attachTaskToConnector(wci.getTaskName(), "DISTRIBUTOR", ConnectorCapability.WRITE);
        }).onVirtualMachine(vmi -> "vm");


        // Define the worker process.
        ts.task((TaskContext tc) -> {
            Pattern pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])");
            ConnectorReader dist = tc.getConnectorManager().getConnectorReader("DISTRIBUTOR");
            int maxLinesBuffered = 50000;
            ConnectorMessage cm = null;
            ArrayList<String> lines = new ArrayList<String>();
            HashMap<String, Integer> mapWords = new HashMap<String, Integer>();
            while (true) {
                int numBuffered = 0;
                lines.clear();
                while ((cm = dist.getValue()) != null && numBuffered < maxLinesBuffered) {
                    lines.add((String) cm.getPayload());
                    numBuffered++;
                }
                if (lines.size() == 0)
                    break;

                PartitionableDataset<Pair<Integer, String>> pd = tc.createPartitionableDataset(new CollectionDataSourceIteratorProvider<String>(lines));
                List<Pair<String, Integer>> mw = pd.withPartitionSize(50000)
                        .mapPairFlat((tdc, line) -> {
                            ArrayList<Pair<String, Integer>> values = new ArrayList<Pair<String, Integer>>();
                            String[] a = pattern.split(line.getV2());
                            HashMap<String, Integer> map = new HashMap<String, Integer>();
                            for (int i = 0; i < a.length; i++) {
                                String word = a[i];
                                if (word.isEmpty())
                                    continue;
                                String w = word.toLowerCase();
                                values.add(new Pair<String, Integer>(w, 1));
                            }
                            return values.iterator();
                        })
                        .reduceByKey((tdc, val1, val2) -> {
                            return val1 + val2;
                        }).collect();
                for (int i = 0; i < mw.size(); i++) {
                    String k = mw.get(i).getV1();
                    int v = mw.get(i).getV2();
                    if (mapWords.containsKey(k))
                        mapWords.put(k, mapWords.get(k) + v);
                    else
                        mapWords.put(k, v);
                }

            }
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
            throw new IllegalArgumentException("Usage: " + ProcessFast_WordCountLineStreamingPD.class.getName() + " <inputDir> <outputDir> <numCores> <maxFilesToRead>");

        long startTime = System.currentTimeMillis();
        MTRuntime runtime = new MTRuntime();
        runtime.setNumThreadsForDataParallelism(Integer.parseInt(args[2]));
        TaskSet ts = createMainTasksSet(runtime, Integer.parseInt(args[2]), args[0], args[1], Integer.parseInt(args[3]));
        runtime.run(ts);
        long endTime = System.currentTimeMillis();
        System.out.println("Done! Execution time: " + (endTime - startTime) + " milliseconds.");
    }
}
