package wordcount;

import it.cnr.isti.hlt.processfast.data.RecursiveFileLineIteratorProvider;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Pattern;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public class Java8StreamWordCountEachFile {
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

    public static void main(String[] args) {
        if (args.length != 3)
            throw new IllegalArgumentException("Usage: " + Java8StreamWordCountEachFile.class.getName() + " <inputDir> <outputDir> <numCores>");

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

        int numFilesToBuffer = 150;
        ArrayList<String> filesBuffered = new ArrayList<>();
        HashMap<String, Integer> mapRes = new HashMap<>();
        ForkJoinPool forkJoinPool = new ForkJoinPool(Integer.parseInt(args[2]));
        try {
            forkJoinPool.submit(() -> {
                int numFilesRead = 0;
                while (true) {
                    filesBuffered.clear();
                    while (numFilesRead < fileList.size() && filesBuffered.size() < numFilesToBuffer)
                        filesBuffered.add(fileList.get(numFilesRead++));
                    if (filesBuffered.size() == 0)
                        break;

                    HashMap<String, Integer> partialResults = filesBuffered.parallelStream().map(filename -> {
                        List<String> articles = readArticles(filename);
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
