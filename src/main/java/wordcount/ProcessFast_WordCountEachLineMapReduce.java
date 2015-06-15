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
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;
import it.cnr.isti.hlt.processfast.data.RamDictionary;
import it.cnr.isti.hlt.processfast.data.RecursiveFileLineIteratorProvider;
import it.cnr.isti.hlt.processfast_mt.core.MTRuntime;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Pattern;


class ProcessFast_WordCountEachLineMapReduce {
    private static Pattern pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])");


    static void runProgram(ProcessfastRuntime runtime, String inputDir, String outputDir) {

        runtime.run(new RamDictionary(), (TaskContext tc) -> {

            PartitionableDataset<String> pd = tc.createPartitionableDataset(new RecursiveFileLineIteratorProvider(inputDir, ""));
            HashMap<String, Integer> mw = pd.withPartitionSize(20000)
                    .map((tdc, line) -> {
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
                    })
                    .reduce((tdc, map1, map2) -> {
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
            throw new IllegalArgumentException("Usage: " + ProcessFast_WordCountEachLineMapReduce.class.getName() + " <inputDir> <outputDir> <numCores>");
        long startTime = System.currentTimeMillis();
        MTRuntime runtime = new MTRuntime();
        runtime.setNumThreadsForDataParallelism(Integer.parseInt(args[2]));
        runProgram(runtime, args[0], args[1]);
        long endTime = System.currentTimeMillis();
        System.out.println("Done! Execution time: " + (endTime - startTime) + " milliseconds.");
    }
}
