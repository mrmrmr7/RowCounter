package service;

import java.io.IOException;

public interface RowCounter {

    Long getRowCount(String path) throws IOException;
}
