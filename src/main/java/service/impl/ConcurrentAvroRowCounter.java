package service.impl;

import com.sun.rowset.internal.Row;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import service.RowCounter;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

public class ConcurrentAvroRowCounter implements RowCounter {
    public Long getRowCount(String path) throws IOException {
        File file = new File(path);

        if (!file.exists()) {
            throw new IOException("File not exists: " + path);
        }

        long count = 0;
        String regex = ".+\\.avro$";
        File[] fileArray = file.listFiles((dir, name) -> Pattern.matches(regex, name));
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        long time = 0;
        long start = System.currentTimeMillis();

        Set<Future<Long>> futureSet = new HashSet<>();

        Arrays.stream(fileArray).forEach(each -> {
            Callable<Long> callable = new FutureRowCount(each);
            futureSet.add(executorService.submit(callable));
        });

        for (Future<Long> each : futureSet) {
            try {
                count += each.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        long finish = System.currentTimeMillis();

        time += (finish - start);

        return count;
    }
}

class FutureRowCount implements Callable<Long> {
    private File file;

    FutureRowCount(File file) {
        this.file = file;
    }

    @Override
    public Long call() throws Exception {
        long count = 0;
        GenericDatumReader datumReader = new GenericDatumReader();
        DataFileReader dataFileReader = new DataFileReader(file, datumReader);

        while (dataFileReader.hasNext()) {
            dataFileReader.next();
            count++;
        }

        return count;
    }
}
