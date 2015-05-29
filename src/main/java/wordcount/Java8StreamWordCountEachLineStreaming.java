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

import it.cnr.isti.hlt.processfast.connector.ConnectorMessage;
import it.cnr.isti.hlt.processfast.data.RecursiveFileLineIteratorProvider;
import it.cnr.isti.hlt.processfast_mt.connector.MTLoadBalancingQueueConnector;
import it.cnr.isti.hlt.processfast_mt.connector.MTTaskLoadBalancingQueueConnector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public class Java8StreamWordCountEachLineStreaming {
    private static Pattern pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])");

    public static void main(String[] args) {
        if (args.length != 3)
            throw new IllegalArgumentException("Usage: " + Java8StreamWordCountEachLineStreaming.class.getName() + " <inputDir> <outputDir> <numCores>");

        long startTime = System.currentTimeMillis();
        RecursiveFileLineIteratorProvider linesProvider = new RecursiveFileLineIteratorProvider(args[0], "");
        Iterator<String> lines = linesProvider.iterator();
        int numLinesToBuffer = 50000;

        HashMap<String, Integer> mapRes = new HashMap<>();
        ForkJoinPool forkJoinPool = new ForkJoinPool(Integer.parseInt(args[2]));
        ExecutorService executorDisk = Executors.newSingleThreadExecutor();
        final MTLoadBalancingQueueConnector connector = new MTLoadBalancingQueueConnector(numLinesToBuffer * 2);
        final MTTaskLoadBalancingQueueConnector diskConnector = new MTTaskLoadBalancingQueueConnector(connector);
        Future diskReader = executorDisk.submit(() -> {
            while (lines.hasNext()) {
                String line = lines.next();
                diskConnector.putValue(line);
            }
            diskConnector.signalEndOfStream();
        });

        while (true) {

            ConnectorMessage cm = null;
            ArrayList<String> linesBuffered = new ArrayList<>();
            while ((cm = diskConnector.getValue()) != null && linesBuffered.size() < numLinesToBuffer)
                linesBuffered.add((String) cm.getPayload());
            if (linesBuffered.size() == 0)
                break;

            try {
                forkJoinPool.submit(() -> {
                    HashMap<String, Integer> partialResults = linesBuffered.parallelStream().filter(line -> {
                        if (line.isEmpty())
                            return false;
                        return !(line.startsWith("<doc") || line.startsWith("</doc"));
                    }).map(line -> {
                        HashMap<String, Integer> map = new HashMap<String, Integer>();
                        String[] a = pattern.split(line);
                        for (int i = 0; i < a.length; i++) {
                            String w = a[i];
                            if (w.isEmpty())
                                continue;
                            w = w.toLowerCase();
                            if (map.containsKey(w))
                                map.put(w, map.get(w) + 1);
                            else
                                map.put(w, 1);
                        }
                        return map;
                    }).reduce((map1, map2) -> {
                        Iterator<String> keys = map2.keySet().iterator();
                        while (keys.hasNext()) {
                            String k = keys.next();
                            int v = map2.get(k);
                            if (map1.containsKey(k))
                                map1.put(k, map1.get(k) + v);
                            else
                                map1.put(k, v);
                        }
                        return map1;
                    }).get();

                    Iterator<String> keys = partialResults.keySet().iterator();
                    while (keys.hasNext()) {
                        String k = keys.next();
                        int v = partialResults.get(k);
                        if (mapRes.containsKey(k))
                            mapRes.put(k, mapRes.get(k) + v);
                        else
                            mapRes.put(k, v);

                    }
                }).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            diskReader.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Write results.
        StringBuilder sb = new StringBuilder();
        Iterator<String> keys = mapRes.keySet().iterator();
        while (keys.hasNext()) {
            String k = keys.next();
            int v = mapRes.get(k);
            sb.append("Word: " + k + " Occurrences: " + v + "\n");
        }
        writeTextFile(args[1] + "\\results.txt", sb.toString());

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
