package service;

import service.impl.HdfsAvroRowCounter;
import service.impl.LocalAvroRowCounter;

public class RowCounterFactory {
    public RowCounter getRowCounter(String key) throws ClassNotFoundException {
        switch (key.toUpperCase().substring(1)) {
            case "L" : return new LocalAvroRowCounter();
            case "H" : return new HdfsAvroRowCounter();
            default: throw new ClassNotFoundException();
        }
    }
}
