package test;

import it.cnr.isti.hlt.processfast.data.RecursiveFileLineIteratorProvider;

import java.util.Iterator;

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
public class TestLinesReader {
    public static void main(String[] args) {
        RecursiveFileLineIteratorProvider reader = new RecursiveFileLineIteratorProvider(args[0], "");
        Iterator<String> it = reader.iterator();
        int counter = 0;
        while (it.hasNext()) {
            String line = it.next();
            counter++;
            if (counter % 100 == 0) {
                System.out.println("Read " + counter + " lines...");
            }
        }
    }
}
