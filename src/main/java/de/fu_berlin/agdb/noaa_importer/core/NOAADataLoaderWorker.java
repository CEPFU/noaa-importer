package de.fu_berlin.agdb.noaa_importer.core;


import de.fu_berlin.agdb.importer.payload.LocationWeatherData;
import org.apache.http.impl.client.SystemDefaultCredentialsProvider;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

public class NOAADataLoaderWorker implements Runnable {

    private static final Logger logger = LogManager.getLogger(NOAADataLoaderWorker.class);

    NQLRunner nqlRunner;

    private IWorkProvider workProvider;

    public NOAADataLoaderWorker(IWorkProvider workProvider) {
        this.workProvider = workProvider;
        nqlRunner = new NQLRunner();
    }

    @Override
    public void run() {
        String fileName = null;
        while((fileName = workProvider.getWork()) != null) {
            try {
                List<LocationWeatherData> dataForLocation = loadDataForFileName(fileName);
                workProvider.deliverResult(dataForLocation);
            } catch (IOException e) {
                logger.error("Error while loading data");
                e.printStackTrace();
            }
        }
    }

    private List<LocationWeatherData> loadDataForFileName(String fileName) throws IOException{
        List<LocationWeatherData> locationWeatherDataList;
        File gribFile = nqlRunner.getNQLResponseFile(fileName, getRecentDirectory());
        if (gribFile != null) {
            WeatherDataFileHandler weatherDataFileHandler = new WeatherDataFileHandler(gribFile);
            locationWeatherDataList = weatherDataFileHandler.handleDataFile();
            // Delete files
            File tmpGbx9File = new File(gribFile.getName() + ".gbx9");
            File tmpNcx3File = new File(gribFile.getName() + ".ncx3");
            gribFile.delete();
            tmpGbx9File.delete();
            tmpNcx3File.delete();
            return locationWeatherDataList;
        }
        return null;
    }

    private String getRecentDirectory() {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        String directory = dateFormat.format(calendar.getTime()) + "00";
        return directory;
    }
}
