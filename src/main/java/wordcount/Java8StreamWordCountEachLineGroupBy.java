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

import it.cnr.isti.hlt.processfast.data.Array;
import it.cnr.isti.hlt.processfast.data.RecursiveFileLineIteratorProvider;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public class Java8StreamWordCountEachLineGroupBy {
    private static Pattern pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])");

    public static void main(String[] args) {
        if (args.length != 3)
            throw new IllegalArgumentException("Usage: " + Java8StreamWordCountEachLineGroupBy.class.getName() + " <inputDir> <outputDir> <numCores>");

        long startTime = System.currentTimeMillis();

        List<String> fileList = new ArrayList<String>();
        for (int mainDir = 0; mainDir < 11; mainDir++) {
            for (int subDir = 0; subDir < 100; subDir++) {
                String fname = String.format(args[0] + "/%03d/wiki_%02d", mainDir, subDir);
                if (new File(fname).exists()) {
                    fileList.add(fname);
                }
            }
        }

        RecursiveFileLineIteratorProvider linesProvider = new RecursiveFileLineIteratorProvider(args[0], "");
        Iterator<String> lines = linesProvider.iterator();
        int numLinesToBuffer = 50000;
        ArrayList<String> linesBuffered = new ArrayList<>();
        HashMap<String, Long> mapRes = new HashMap<>();
        ForkJoinPool forkJoinPool = new ForkJoinPool(Integer.parseInt(args[2]));
        try {
            forkJoinPool.submit(() -> {
                while (true) {
                    linesBuffered.clear();
                    while (lines.hasNext() && linesBuffered.size() < numLinesToBuffer)
                        linesBuffered.add(lines.next());
                    if (linesBuffered.size() == 0)
                        break;

                    Map<String, Long> partialResults = linesBuffered.parallelStream().filter(line -> {
                        if (line.isEmpty())
                            return false;
                        return !(line.startsWith("<doc") || line.startsWith("</doc"));
                    }).flatMap(line -> {
                    /*if (line.isEmpty())
                        return new HashMap<String, Integer>();
                    if (line.startsWith("<doc") || line.startsWith("</doc"))
                        return new HashMap<String, Integer>();*/

                        ArrayList<String> words = new ArrayList<String>();
                        String[] a = pattern.split(line);
                        for (int i = 0; i < a.length; i++) {
                            String w = a[i];
                            if (w.isEmpty())
                                continue;
                            w = w.toLowerCase();
                            words.add(w);
                        }
                        return words.parallelStream();
                    }).collect(Collectors.groupingBy(w -> w, Collectors.counting()));

                    Iterator<String> keys = partialResults.keySet().iterator();
                    while (keys.hasNext()) {
                        String k = keys.next();
                        long v = partialResults.get(k);
                        if (mapRes.containsKey(k))
                            mapRes.put(k, mapRes.get(k) + v);
                        else
                            mapRes.put(k, v);
                    }
                }

                // Write results.
                StringBuilder sb = new StringBuilder();
                Iterator<String> keys = mapRes.keySet().iterator();
                while (keys.hasNext()) {
                    String k = keys.next();
                    long v = mapRes.get(k);
                    sb.append("Word: " + k + " Occurrences: " + v + "\n");
                }
                writeTextFile(args[1] + "\\results.txt", sb.toString());

                long endTime = System.currentTimeMillis();
                System.out.println("Done! Execution time: " + (endTime - startTime) + " milliseconds.");
            }).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeTextFile(String filename, String textToWrite) {
        try {
            new File(filename).getParentFile().mkdirs();
            FileWriter writer = new FileWriter(filename, true);
            BufferedWriter bufferedWriter = new BufferedWriter(writer);
            bufferedWriter.write(textToWrite);
            bufferedWriter.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
