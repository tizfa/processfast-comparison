package pagerank;

import it.cnr.isti.hlt.processfast.core.TaskSet;
import it.cnr.isti.hlt.processfast.data.*;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast_mt.core.MTProcessfastRuntime;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public class ProcessFastPageRank {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: ProcessFastPageRank <file> <outFile> <numCores>");
            System.exit(1);
        }

        String inputFile = args[0];
        String outFile = args[1];

        MTProcessfastRuntime runtime = new MTProcessfastRuntime();
        runtime.setNumThreadsForDataParallelism(Integer.parseInt(args[2]));
        //runtime.setNumThreadsForDataParallelism(1);
        TaskSet ts = runtime.createTaskSet();
        ts.getDataDictionary().put("inputFile", args[0]);

        long startTime = System.currentTimeMillis();
        ts.task((ctx) -> {

            // Load documents to clusterize from the input storage.
            String inFile = ctx.getTasksSetDataDictionary().get("inputFile");
            PartitionableDataset<IndexFileLineIteratorProvider.LineInfo> lines = ctx.createPartitionableDataset(new IndexFileLineIteratorProvider(inputFile));
            PairPartitionableDataset<String, DataIterable<String>> links = lines.withPartitionSize(100000).filter((c, li) -> !li.line.startsWith("#")).mapPair((c, li) -> {
                String[] parts = li.line.split("\\s+");
                return new Pair<String, String>(parts[0], parts[1]);
            }).distinct().groupByKey().cache(CacheType.RAM);
            PairPartitionableDataset<String, Double> ranks = links.mapValues((c, v) -> 1.0);

            int numIterations = 2;
            for (int i = 0; i < numIterations; i++) {
                PairPartitionableDataset<String, Double> contribs = links.join(ranks).values().mapFlat((c, v) -> {
                    DataIterable<String> urls = v.getV1();
                    double rank = v.getV2();
                    Iterator<String> urlIt = urls.iterator();
                    int count = 0;
                    while (urlIt.hasNext()) {
                        urlIt.next();
                        count++;
                    }
                    urlIt = urls.iterator();
                    ArrayList<Pair<String, Double>> ret = new ArrayList<Pair<String, Double>>();
                    while (urlIt.hasNext()) {
                        ret.add(new Pair<String, Double>(urlIt.next(), rank / count));
                    }
                    return ret.iterator();
                }).mapPair((c, v) -> v);

                ranks = contribs.reduceByKey((c, v1, v2) -> v1 + v2)
                        .mapValues((c, v) -> 0.15 + 0.85 * v).cache(CacheType.RAM);
                System.out.println("At the end of iteration " + (i + 1));
            }
            List<Pair<String, Double>> results = ranks.collect();
            ArrayList<Pair<String, Double>> res = new ArrayList<Pair<String, Double>>(results);

            ctx.getTasksSetDataDictionary().put("results", res);
            ctx.getLogManager().getLogger("kmeans").info("KMeans computation done!");
        });

        runtime.run(ts);
        ArrayList<Pair<String, Double>> ret = ts.getDataDictionary().get("results");
        StringBuilder sb = new StringBuilder();
        for (Pair<String, Double> doc : ret) {
            sb.append(doc.getV1() + " has rank: " + doc.getV2() + "\n");
        }
        writeTextFile(args[1], sb.toString());

        long endTime = System.currentTimeMillis();
        System.out.println("Computation time: " + (endTime - startTime) + " milliseconds\n");
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
