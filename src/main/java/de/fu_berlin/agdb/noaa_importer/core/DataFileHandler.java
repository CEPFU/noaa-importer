package de.fu_berlin.agdb.noaa_importer.core;

import de.fu_berlin.agdb.importer.payload.LocationWeatherData;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.IOException;
import java.util.List;

public abstract class DataFileHandler {

    private File file;

    public DataFileHandler(File file) {
        this.file = file;
    }

    public abstract List<LocationWeatherData> handleDataFile();

    protected File getFile() {
        return file;
    }
}
