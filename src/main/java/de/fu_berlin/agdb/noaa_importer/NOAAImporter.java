package de.fu_berlin.agdb.noaa_importer;

import de.fu_berlin.agdb.importer.AWeatherImporter;
import de.fu_berlin.agdb.importer.payload.LocationMetaData;
import de.fu_berlin.agdb.importer.payload.LocationWeatherData;
import de.fu_berlin.agdb.noaa_importer.core.IWorkProvider;
import de.fu_berlin.agdb.noaa_importer.core.NOAADataLoaderWorker;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class NOAAImporter extends AWeatherImporter implements IWorkProvider {

    private static final Logger logger = LogManager.getLogger(NOAAImporter.class);

    private static final int NUMBER_OF_THREADS = 10;

    private List<Thread> threadPool;

    private List<LocationWeatherData> accumulatedData;
    private List<String> gribFilesToBeDone;

    public NOAAImporter() {
        threadPool = new ArrayList<Thread>();
    }

    @Override
    protected List<LocationWeatherData> getWeatherDataForLocations(List<LocationMetaData> locations) {
        gribFilesToBeDone = generateFileNames();

        accumulatedData = new ArrayList<LocationWeatherData>(1419);

        for (int threadCount = 0; threadCount < NUMBER_OF_THREADS; threadCount++) {
            Thread thread = new Thread(new NOAADataLoaderWorker(this));
            threadPool.add(thread);
            thread.start();
        }

        while (!threadPool.isEmpty()) {
            try {
                Thread thread = threadPool.get(0);
                thread.join();
                threadPool.remove(thread);
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for thread");
            }
        }

        return accumulatedData;
    }

    @Override
    protected long getServiceTimeout() {
        // 24 hours
        return 24*60*60*1000;
    }

    @Override
    public String getWork() {
        synchronized (gribFilesToBeDone) {
            if (!gribFilesToBeDone.isEmpty()) {
                String work = gribFilesToBeDone.get(0);
                gribFilesToBeDone.remove(work);
                return work;
            } else {
                return null;
            }
        }
    }

    @Override
    public void deliverResult(List<LocationWeatherData> dataForLocation) {
        synchronized (accumulatedData) {
            if (dataForLocation != null) {
                accumulatedData.addAll(dataForLocation);
            }
        }
    }

    private List<String> generateFileNames() {
        List<String> fileNames = new ArrayList<String>();
        int forecastHours = 384;
        while (forecastHours > 0) {
            String baseFileName = "gfs.t00z.pgrb2.0p25.f";
            fileNames.add(baseFileName + String.format("%03d", forecastHours));
            if (forecastHours > 240) {
                forecastHours = forecastHours - 12;
            }
            else {
                forecastHours = forecastHours - 3;
            }
        }
        return fileNames;
    }
}
