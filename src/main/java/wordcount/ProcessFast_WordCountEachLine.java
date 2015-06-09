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
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;
import it.cnr.isti.hlt.processfast.data.RamDictionary;
import it.cnr.isti.hlt.processfast.data.RecursiveFileLineIteratorProvider;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast_mt.core.MTRuntime;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;


class ProcessFast_WordCountEachLine {
    private static Pattern pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])");


    static void runProgram(ProcessfastRuntime runtime, String inputDir, String outputDir) {

        runtime.run(new RamDictionary(), (TaskContext tc) -> {

            System.out.println("Ok, sono qui");
            PartitionableDataset<String> pd = tc.createPartitionableDataset(new RecursiveFileLineIteratorProvider(inputDir, ""));
            List<Pair<String, Integer>> mw = pd.withPartitionSize(50000)
                    .filter((tdc, line) -> {
                        if (line.isEmpty())
                            return false;
                        return !(line.startsWith("<doc") || line.startsWith("</doc"));
                    })
                    .mapPairFlat((tdc, line) -> {
                        ArrayList<Pair<String, Integer>> values = new ArrayList<Pair<String, Integer>>();
                        String[] a = pattern.split(line);
                        HashMap<String, Integer> map = new HashMap<String, Integer>();
                        for (int i = 0; i < a.length; i++) {
                            String word = a[i];
                            if (word.isEmpty())
                                continue;
                            String w = word.toLowerCase();
                            values.add(new Pair<>(w, 1));
                        }
                        return values.iterator();
                    })
                    .reduceByKey((tdc, val1, val2) -> {
                        return val1 + val2;
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
            throw new IllegalArgumentException("Usage: " + ProcessFast_WordCountEachLine.class.getName() + " <inputDir> <outputDir> <numCores>");
        long startTime = System.currentTimeMillis();
        MTRuntime runtime = new MTRuntime();
        runtime.setNumThreadsForDataParallelism(Integer.parseInt(args[2]));
        runProgram(runtime, args[0], args[1]);
        long endTime = System.currentTimeMillis();
        System.out.println("Done! Execution time: " + (endTime - startTime) + " milliseconds.");
    }
}
