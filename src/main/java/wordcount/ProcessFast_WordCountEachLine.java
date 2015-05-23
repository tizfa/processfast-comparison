package wordcount;

import it.cnr.isti.hlt.processfast.core.ProcessfastRuntime;
import it.cnr.isti.hlt.processfast.core.TaskContext;
import it.cnr.isti.hlt.processfast.data.PartitionableDataset;
import it.cnr.isti.hlt.processfast.data.RamDictionary;
import it.cnr.isti.hlt.processfast.data.RecursiveFileLineIteratorProvider;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast_gpars.core.GParsRuntime;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;


class ProcessFast_WordCountEachLine {
    private static Pattern pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])");


    static void runProgram(ProcessfastRuntime runtime, String inputDir, String outputDir) {

        runtime.run(new RamDictionary(), (TaskContext tc) -> {

            PartitionableDataset<String> pd = tc.createPartitionableDataset(new RecursiveFileLineIteratorProvider(inputDir, ""));
            List<Pair<String, Integer>> mw = pd.withPartitionSize(15000)
                    .mapPairFlat((tdc, line) -> {
                        ArrayList<Pair<String, Integer>> values = new ArrayList<Pair<String, Integer>>();
                        String[] a = pattern.split(line);
                        HashMap<String, Integer> mapValues = new HashMap<String, Integer>();
                        for (int i = 0; i < a.length; i++) {
                            String word = a[i];
                            if (word.isEmpty())
                                continue;
                            String w = word.toLowerCase();
                            if (mapValues.containsKey(w))
                                mapValues.put(w, mapValues.get(w) + 1);
                            else
                                mapValues.put(w, 1);
                        }

                        Iterator<String> keys = mapValues.keySet().iterator();
                        while (keys.hasNext()) {
                            String k = keys.next();
                            int v = mapValues.get(k);
                            values.add(new Pair<>(k, v));
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
        GParsRuntime runtime = new GParsRuntime();
        runtime.setNumThreadsForDataParallelism(Integer.parseInt(args[2]));
        runProgram(runtime, args[0], args[1]);
        long endTime = System.currentTimeMillis();
        System.out.println("Done! Execution time: " + (endTime - startTime) + " milliseconds.");
    }
}
