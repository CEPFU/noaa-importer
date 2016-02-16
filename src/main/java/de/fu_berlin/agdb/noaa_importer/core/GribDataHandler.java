package de.fu_berlin.agdb.noaa_importer.core;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

public class GribDataHandler implements IWeatherDataFileProvider {

    private static final Logger logger = LogManager.getLogger(GribDataHandler.class);

    private File weatherDataFile;

    public GribDataHandler(File file) throws IOException {
        handle(file);
    }

    @Override
    public File getWeatherDataFile() {
        return weatherDataFile;
    }

    private void handle(File file) throws IOException {
        weatherDataFile = file;
    }

}
