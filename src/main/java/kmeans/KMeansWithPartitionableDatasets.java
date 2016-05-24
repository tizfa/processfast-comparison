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

package kmeans;

import it.cnr.isti.hlt.processfast.core.TaskContext;
import it.cnr.isti.hlt.processfast.core.TaskSet;
import it.cnr.isti.hlt.processfast.data.*;
import it.cnr.isti.hlt.processfast.utils.Pair;
import it.cnr.isti.hlt.processfast.utils.Triple;
import it.cnr.isti.hlt.processfast_mt.core.MTRuntime;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * In this example we use k-means algorithm to clusterize a big collection of
 * documents. Each document is composed by a list of unique features, and each
 * feature of a document has a specific weight between 0 and 1.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public class KMeansWithPartitionableDatasets {

    /**
     * The number of clusters used to divide the input data.
     */
    private final static int NUM_CLUSTERS = 10;

    /**
     * The number of iterations performed by the algorithm.
     */
    private final static int NUM_ITERATIONS = 50;

    /**
     * The input storage name.
     */
    private final static String INPUT_STORAGE_NAME = "inputStorageName";

    /**
     * The data stream name in input storage.
     */
    private final static String INPUT_DATASTREAM_NAME = "dataStreamName";

    /**
     * Temporary storage used to perform the required operations.
     */
    private final static String TMP_STORAGE_NAME = "_tmp_storage_";


    /**
     * The output storage name.
     */
    //private final static String OUTPUT_STORAGE_NAME = "outputStorageName";

    /**
     * The prefix to apply while naming the tmp arrays storing clusters
     * centroids.
     */
    private final static String TMP_CLUSTER_ARRAY_PREFIX_NAME = "__tmpCluster_";

    public static class FeatInfo implements Serializable {
        int featureID;
        double featWeight;

        FeatInfo(int id, double weight) {
            featureID = id;
            featWeight = weight;
        }
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: " + KMeansWithPartitionableDatasets.class.getName() + " <inputFile> <outFile> <numCores>");
            System.exit(-1);
        }

        MTRuntime runtime = new MTRuntime();
        runtime.setNumThreadsForDataParallelism(Integer.parseInt(args[2]));
        TaskSet ts = runtime.createTaskSet();
        ts.getDataDictionary().put("inputFile", args[0]);

        ts.task((ctx) -> {

            // Load documents to clusterize from the input storage.
            String inputFile = ctx.getTasksSetDataDictionary().get("inputFile");
            PairPartitionableDataset<Integer, DataIterable<FeatInfo>> documents = loadDocuments(ctx, inputFile);
            long numDocuments = documents.count();
            List<Integer> clustersAssignment = createInitialClustersAssignment(
                    numDocuments, NUM_CLUSTERS);

            // Create tmp structures for clusters centroid.
            createTmpClustersCentroidsStructures(ctx, NUM_CLUSTERS);

            // Compute initial centroids.
            PairPartitionableDataset<Integer, DataIterable<FeatInfo>> centroids = updateCentroids(
                    ctx,
                    documents,
                    ctx.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider<>(
                            clustersAssignment)));

            // Compute KMeans iteratively.
            List<Pair<Integer, Integer>> bestClusters = null;
            for (int i = 0; i < NUM_ITERATIONS; i++) {
                // Compute best clusters. Each returned pair is (doc_id,
                // assigned_cluster_id).
                bestClusters = computeBestClusters(
                        documents, centroids);
                if (i >= NUM_ITERATIONS)
                    break;

                // Reassign documents to clusters.
                clustersAssignment.clear();
                for (Pair<Integer, Integer> v : bestClusters) {
                    clustersAssignment.set(v.getV1(), v.getV2());
                }

                // Update cluster centroids. Each pair is (id_cluster, list_of_features)
                centroids = updateCentroids(
                        ctx,
                        documents,
                        ctx.createPairPartitionableDataset(new CollectionDataSourceIteratorProvider<>(
                                clustersAssignment)));

                ctx.getLogManager().getLogger("kmeans").info("Performed iteration <" + (i + 1) + ">...");
            }

            // Delete tmp clusters centroids.
            dropTmpClustersCentroidsStructures(ctx, NUM_CLUSTERS);

            // Return results using system global dictionary.
            ArrayList<Pair<Integer, Integer>> ret = new ArrayList<>(bestClusters);
            ctx.getTasksSetDataDictionary().put("results", ret);
            ctx.getLogManager().getLogger("kmeans").info("KMeans computation done!");
        });

        runtime.run(ts);

        // Each item is <doc_id, cluster_id>
        ArrayList<Pair<Integer, Integer>> ret = ts.getDataDictionary().get("results");
        StringBuilder sb = new StringBuilder();
        for (Pair<Integer, Integer> doc : ret) {
            sb.append("DocID: " + doc.getV1() + ", ClusterID: " + doc.getV2() + "\n");
        }
        writeTextFile(args[1], sb.toString());
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

    private static List<Pair<Integer, Integer>> computeBestClusters(
            PairPartitionableDataset<Integer, DataIterable<FeatInfo>> documents,
            PairPartitionableDataset<Integer, DataIterable<FeatInfo>> centroids) {
        List<Pair<Integer, Integer>> bestClusters = documents
                .cartesian(centroids)
                .map((context, v) -> { // Compute similarity between a
                    // document and a cluster.
                    Pair<Integer, DataIterable<FeatInfo>> doc = v.getV1();
                    Pair<Integer, DataIterable<FeatInfo>> centroid = v.getV2();
                    double similarity = computeSimilarity(doc, centroid);
                    return new Triple<>(doc.getV1(),
                            centroid.getV1(), similarity);
                })
                .groupBy((context, v) -> { // Group clusters
                    // similarity values by
                    // document.
                    return v.getV1();
                })
                .map((context, d) -> { // Choose the best cluster for a
                    // specific document.
                    DataIterable<Triple<Integer, Integer, Double>> similaritiesIt = d
                            .getV2();

                    Iterator<Triple<Integer, Integer, Double>> similarities = similaritiesIt.iterator();
                    // Compute best cluster.
                    int bestCluster = 0;
                    double bestSimilarity = 0;
                    while (similarities.hasNext()) {
                        Triple<Integer, Integer, Double> val = similarities
                                .next();
                        if (val.getV3() > bestSimilarity) {
                            bestCluster = val.getV2();
                            bestSimilarity = val.getV3();
                        }
                    }

                    // Return the most similar cluster to this document.
                    return new Pair<>(d.getV1(), bestCluster);
                }).collect(); // Assuming this list can be stored on
        // RAM. If not, cache results on disk
        // and access it
        // later by using take() and count()
        // methods.
        return bestClusters;
    }

    private static void createTmpClustersCentroidsStructures(TaskContext ctx,

                                                             int numClusters) {
        Storage storage = ctx.getStorageManager().createStorage(
                TMP_STORAGE_NAME);
        for (int i = 0; i < numClusters; i++) {
            storage.createArray(TMP_CLUSTER_ARRAY_PREFIX_NAME + i,
                    FeatInfo.class);
        }
    }

    private static void dropTmpClustersCentroidsStructures(TaskContext ctx,
                                                           int numClusters) {
        Storage storage = ctx.getStorageManager().getStorage(
                TMP_STORAGE_NAME);
        for (int i = 0; i < numClusters; i++) {
            storage.removeArray(TMP_CLUSTER_ARRAY_PREFIX_NAME + i);
        }
    }

    private static List<Integer> createInitialClustersAssignment(
            long numDocuments, int numClusters) {
        Random r = new Random();
        ArrayList<Integer> a = new ArrayList<>();
        for (int i = 0; i < numDocuments; i++) {
            a.add(r.nextInt(numClusters));
        }
        return a;
    }

    /**
     * Compute cosine similarity.
     */
    private static double computeSimilarity(
            Pair<Integer, DataIterable<FeatInfo>> doc,
            Pair<Integer, DataIterable<FeatInfo>> centroid) {
        double similarity = 0;
        double squareCentroid = 0;
        double squareDoc = 0;
        FeatInfo endList = new FeatInfo(Integer.MAX_VALUE, 0);

        Iterator<FeatInfo> itCentroid = centroid.getV2().iterator();
        Iterator<FeatInfo> itDoc = doc.getV2().iterator();
        FeatInfo fiCentroid = itCentroid.next();
        FeatInfo fiDoc = itDoc.next();
        boolean done = false;
        while (!done) {
            boolean updateCentroid = false;
            boolean updateDoc = false;
            if (fiCentroid.featureID == fiDoc.featureID) {
                similarity += fiCentroid.featWeight * fiDoc.featWeight;
                squareCentroid += fiCentroid.featWeight * fiCentroid.featWeight;
                squareDoc += fiDoc.featWeight * fiDoc.featWeight;
                updateCentroid = true;
                updateDoc = true;
            } else if (fiCentroid.featureID < fiDoc.featureID) {
                squareCentroid += fiCentroid.featWeight * fiCentroid.featWeight;
                updateCentroid = true;
            } else {
                squareDoc += fiDoc.featWeight * fiDoc.featWeight;
                updateDoc = true;
            }

            if (updateCentroid) {
                if (itCentroid.hasNext())
                    fiCentroid = itCentroid.next();
                else
                    fiCentroid = endList;
            }
            if (updateDoc) {
                if (itDoc.hasNext())
                    fiDoc = itDoc.next();
                else
                    fiDoc = endList;
            }

            if (fiDoc.featureID == Integer.MAX_VALUE
                    && fiCentroid.featureID == Integer.MAX_VALUE)
                done = true;
        }

        similarity = similarity
                / (Math.sqrt(squareCentroid) * Math.sqrt(squareDoc));
        return similarity;
    }


    private static PairPartitionableDataset<Integer, DataIterable<FeatInfo>> updateCentroids(
            TaskContext ctx,
            PairPartitionableDataset<Integer, DataIterable<FeatInfo>> documents,
            PairPartitionableDataset<Integer, Integer> assignment) {

        List<Pair<Integer, DataIterable<DataIterable<FeatInfo>>>> centroids = assignment.enableLocalComputation(true)
                .join(documents) // Join the datasets using the same document_id.
                .mapPair((context, v) -> { // Remove document ID from values.
                    return v.getV2();
                }).groupByKey() // Group items by cluster ID.
                .collect(); // Take the results in memory, assuming the list of documents
        // for each cluster is loaded when required.

        Storage storage = ctx.getStorageManager().getStorage(
                TMP_STORAGE_NAME);

        for (Pair<Integer, DataIterable<DataIterable<FeatInfo>>> centroid : centroids) {
            // Work on 1 cluster at a time.
            Iterator<FeatInfo> ce = new ArrayList<FeatInfo>().iterator();
            int clusterID = centroid.getV1();

            // Computer cluster centroid incrementally.
            DataIterable<DataIterable<FeatInfo>> docsIt = centroid.getV2();
            Iterator<DataIterable<FeatInfo>> docs = docsIt.iterator();
            int numDocs = 1;
            while (docs.hasNext()) {
                DataIterable<FeatInfo> items = docs.next();
                Iterator<FeatInfo> doc = items.iterator();
                ce = updateCentroidWithDocument(ce, doc, numDocs);
                numDocs++;
            }

            Array<FeatInfo> ceOut = storage
                    .getArray(TMP_CLUSTER_ARRAY_PREFIX_NAME + clusterID, FeatInfo.class);

            // Write final centroid on corresponding array in the output
            // storage.
            ceOut.clear();
            while (ce.hasNext()) {
                ceOut.appendValue(ce.next());
            }
        }

        // Ensure that all updated data is flushed on storage.
        storage.flushData();

        // Create a partitionable dataset containing all updated clusters.
        ArrayList<ImmutableDataSourceIteratorProvider<FeatInfo>> sources = new ArrayList<>();
        for (int i = 0; i < centroids.size(); i++) {
            Array<FeatInfo> ce = storage.getArray(TMP_CLUSTER_ARRAY_PREFIX_NAME
                    + i, FeatInfo.class);
            sources.add(ce.asIteratorProvider(100));
        }
        return ctx.createPairPartitionableDataset(sources.iterator());
    }

    private static Iterator<FeatInfo> updateCentroidWithDocument(
            Iterator<FeatInfo> ce, Iterator<FeatInfo> doc, int numDocs) {
        boolean done = false;
        FeatInfo endList = new KMeansWithPartitionableDatasets.FeatInfo(Integer.MAX_VALUE, 0);
        FeatInfo fiCentroid = ce.hasNext() ? ce.next() : endList;
        FeatInfo fiDocument = doc.hasNext() ? doc.next() : endList;
        ArrayList<FeatInfo> ceOut = new ArrayList<>();
        while (!done) {
            if (fiCentroid.featureID == fiDocument.featureID) {
                double updatedValue = fiCentroid.featWeight
                        + (fiDocument.featWeight - fiCentroid.featWeight)
                        / numDocs;
                FeatInfo fi = new FeatInfo(fiCentroid.featureID, updatedValue);
                ceOut.add(fi);
                if (doc.hasNext())
                    fiDocument = doc.next();
                else
                    fiDocument = endList;

                if (ce.hasNext())
                    fiCentroid = ce.next();
                else
                    fiCentroid = endList;
            } else if (fiCentroid.featureID < fiDocument.featureID) {
                ceOut.add(fiCentroid);
                if (ce.hasNext())
                    fiCentroid = ce.next();
                else
                    fiCentroid = endList;

            } else {
                ceOut.add(fiDocument);
                if (doc.hasNext())
                    fiDocument = doc.next();
                else
                    fiDocument = endList;
            }

            if (fiCentroid.featureID == Integer.MAX_VALUE
                    && fiDocument.featureID == Integer.MAX_VALUE)
                done = true;
        }

        return ceOut.iterator();
    }

    /**
     * Load documents from a textual stream stored on storage manager.
     *
     * @param ctx The runtime context.
     * @return The set of documents loaded.
     */
    private static PairPartitionableDataset<Integer, DataIterable<FeatInfo>> loadDocuments(
            TaskContext ctx, String inputFile) {
        PartitionableDataset<IndexFileLineIteratorProvider.LineInfo> documents = ctx.createPartitionableDataset(new IndexFileLineIteratorProvider(inputFile));

        // The source stream must have the svm_light format.
        PairPartitionableDataset<Integer, DataIterable<FeatInfo>> ret = documents
                .mapPair( // Parse each line of text in SVM_LIGHT format.
                        (context, v) -> {
                            ArrayList<FeatInfo> vect = new ArrayList<>();
                            int docIdx = v.lineIndex;
                            String line = v.line;
                            String[] features = line.split("\\s");
                            for (int i = 1; i < features.length; i++) {
                                String feat = features[i];
                                if (feat.isEmpty())
                                    continue;
                                if (feat.startsWith("#"))
                                    break;
                                String[] fi = feat.split(":");
                                if (fi.length == 0)
                                    continue;
                                vect.add(new FeatInfo(Integer.parseInt(fi[0]),
                                        Double.parseDouble(fi[1])));
                            }
                            return new Pair<Integer, DataIterable<FeatInfo>>(
                                    docIdx, new CollectionDataIterable<>(vect));
                        }).cache(CacheType.ON_DISK);
        return ret;
    }

}
