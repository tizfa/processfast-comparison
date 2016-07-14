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
import it.cnr.isti.hlt.processfast.data.RecursiveFileLineIteratorProvider;
import it.cnr.isti.hlt.processfast_mt.core.MTProcessfastRuntime;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;


class ProcessFast_WordCountLineStreaming {


    static TaskSet createMainTasksSet(ProcessfastRuntime runtime, int numWorkers, String inputDir, String outputDir, int maxLinesToRead) {
        // Create main tasks set.
        TaskSet ts = runtime.createTaskSet();
        ts.getDataDictionary().put("INPUT_DIR", inputDir);
        ts.getDataDictionary().put("OUTPUT_DIR", outputDir);

        ts.createConnector("DISTRIBUTOR", ConnectorType.LOAD_BALANCING_QUEUE, maxLinesToRead);
        ts.createConnector("COLLECTOR", ConnectorType.LOAD_BALANCING_QUEUE, 50);
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
            ConnectorWriter coll = tc.getConnectorManager().getConnectorWriter("COLLECTOR");
            HashMap<String, Integer> mapWords = new HashMap<String, Integer>();
            int tickLastSend = 0;
            while (true) {
                ConnectorMessage cm = dist.getValue();
                if (cm == null)
                    break;

                String line = (String) cm.getPayload();
                String[] a = pattern.split(line);
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
                tickLastSend++;
                if (tickLastSend >= 2000) {
                    coll.putValue(mapWords);
                    mapWords = new HashMap<String, Integer>();
                    tickLastSend = 0;
                }
            }

            coll.putValue(mapWords);

            tc.barrier("WORKER_BARRIER");
            if (tc.getInstanceNumber() == 0)
                coll.signalEndOfStream();
            //}).withNumInstances(6, 8).withBarriers(
        }).withNumInstances(numWorkers, numWorkers + 1).withBarriers(
                wbi -> {
                    ArrayList<String> l = new ArrayList<String>();
                    l.add("WORKER_BARRIER");
                    return l.iterator();
                }
        ).withConnectors(wci -> {
            wci.getConnectorManager().attachTaskToConnector(wci.getTaskName(), "DISTRIBUTOR", ConnectorCapability.READ);
            wci.getConnectorManager().attachTaskToConnector(wci.getTaskName(), "COLLECTOR", ConnectorCapability.WRITE);
        }).onVirtualMachine(
                vmi -> "vm"
        );

        // Collector.
        ts.task((TaskContext tc) -> {
            long startTime = System.currentTimeMillis();
            ConnectorReader coll = tc.getConnectorManager().getConnectorReader("COLLECTOR");
            HashMap<String, Integer> wordsRes = new HashMap<>(2000000);
            while (true) {
                ConnectorMessage cm = coll.getValue();
                if (cm == null)
                    break;
                HashMap<String, Integer> res = (HashMap<String, Integer>) cm.getPayload();
                Iterator<String> keys = res.keySet().iterator();
                while (keys.hasNext()) {
                    String k = keys.next();
                    int v = res.get(k);
                    if (wordsRes.containsKey(k))
                        wordsRes.put(k, wordsRes.get(k) + v);
                    else
                        wordsRes.put(k, v);
                }
            }

            // Write results.
            StringBuilder sb = new StringBuilder();
            Iterator<String> keys = wordsRes.keySet().iterator();
            while (keys.hasNext()) {
                String k = keys.next();
                int v = wordsRes.get(k);
                sb.append("Word: " + k + " Occurrences: " + v + "\n");
            }
            writeTextFile(tc.getTasksSetDataDictionary().get("OUTPUT_DIR") + "/results.txt", sb.toString());
            long endTime = System.currentTimeMillis();
            tc.getLogManager().getLogger("test").info("Done! Execution time: " + (endTime - startTime) + " milliseconds.");
        }).withConnectors(wci ->
                        wci.getConnectorManager().attachTaskToConnector(wci.getTaskName(), "COLLECTOR", ConnectorCapability.READ)
        ).onVirtualMachine(
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
            throw new IllegalArgumentException("Usage: " + ProcessFast_WordCountLineStreaming.class.getName() + " <inputDir> <outputDir> <numCores> <maxFilesToRead>");

        MTProcessfastRuntime runtime = new MTProcessfastRuntime();
        TaskSet ts = createMainTasksSet(runtime, Integer.parseInt(args[2]), args[0], args[1], Integer.parseInt(args[3]));
        runtime.run(ts);
    }
}
