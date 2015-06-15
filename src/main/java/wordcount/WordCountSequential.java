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

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public class WordCountSequential {

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

    public static void main(String[] args) {
        if (args.length != 2)
            throw new IllegalArgumentException("Usage: " + WordCountSequential.class.getName() + " <inputDir> <outputDir>");

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

        Pattern pattern = Pattern.compile("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\#\\*\\(\\)\\=\\?\\^\\!\\|])");
        HashMap<String, Integer> mapWords = new HashMap<String, Integer>();
        for (int idFile = 0; idFile < fileList.size(); idFile++) {
            List<String> articles = readArticles(fileList.get(idFile));
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
            System.out.println("Analyzed file " + (idFile + 1) + " - " + fileList.get(idFile) + "...");
        }

        // Write results.
        StringBuilder sb = new StringBuilder();
        Iterator<String> keys = mapWords.keySet().iterator();
        while (keys.hasNext()) {
            String k = keys.next();
            int v = mapWords.get(k);
            sb.append("Word: " + k + " Occurrences: " + v + "\n");
        }
        writeTextFile(args[1] + "/results.txt", sb.toString());

        long endTime = System.currentTimeMillis();
        System.out.println("Done! Execution time: " + (endTime - startTime) + " milliseconds.");
    }
}
