
import service.RowCounter;
import service.impl.AvroRowCounter;

import java.io.FileNotFoundException;
import java.io.IOException;

public class AvroRowCount {
    public static void main(String[] args) {
        RowCounter rowCounter = new AvroRowCounter();

        if (args[0] != null) {
            try {
                long count = 0;

                long start = System.currentTimeMillis();
                count = rowCounter.getRowCount(args[0]);
                long end = System.currentTimeMillis();

                System.out.println("Count: " + count);
                System.out.println(" Time: " + (end - start) / 1000. + " sec.");
            } catch (FileNotFoundException e) {
                System.out.println("Error: file not exists");
            } catch (IOException e) {
                System.out.println("Error: not valid file");
            }
        } else {
            System.out.println("Error: not enough parameters");
        }


    }
}