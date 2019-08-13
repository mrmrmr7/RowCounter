package service.impl;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import service.RowCounter;

import java.io.IOException;

public class HdfsAvroRowCounter implements RowCounter {
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

    @Override
    public Long getRowCount(String path) throws IOException {
        Configuration configuration = new Configuration();
        long count = 0;

        FsInput fsInput = new FsInput(
                new Path(path),
                configuration
        );

        count = countRows(fsInput);

        return count;
    }


    private Long countRows(FsInput file) throws IOException {
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);
        long count = 0;

        while (dataFileReader.hasNext()) {
            dataFileReader.next();
            count++;
        }

        dataFileReader.close();
        return count;
    }
}
