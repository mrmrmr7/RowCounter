package service.impl;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import service.RowCounter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.regex.Pattern;

public class AvroRowCounter implements RowCounter {
    public Long getRowCount(String path) throws IOException {
        File file = new File(path);

        if (!file.exists()) {
            throw new FileNotFoundException();
        }

        long count = 0;
        String regex = ".+\\.avro$";
        if (file.isDirectory()) {
            File[] fileArray = file.listFiles((dir, name) -> Pattern.matches(regex, name));

            for (File each : fileArray) {
                count += countRows(each);
            }
        } else if (Pattern.matches(regex, path) && file.length() != 0) {
            count = countRows(file);
        }

        return count;
    }

    private Long countRows(File file) throws IOException {
        long count = 0;
        GenericDatumReader datumReader = new GenericDatumReader();
        DataFileReader dataFileReader;

        dataFileReader = new DataFileReader(file, datumReader);

        while (dataFileReader.hasNext()) {
            dataFileReader.next();
            count++;
        }

        return count;
    }
}
