package de.fu_berlin.agdb.noaa_importer.core;

import java.io.File;
import java.io.IOException;

public abstract class DataFileHandler {

    private File file;

    public DataFileHandler(File file) {
        this.file = file;
    }

    public abstract void handleDataFile() throws IOException;

    protected File getFile() {
        return file;
    }
}
