package de.fu_berlin.agdb.noaa_importer;

import de.fu_berlin.agdb.importer.AWeatherImporter;
import de.fu_berlin.agdb.importer.payload.LocationMetaData;
import de.fu_berlin.agdb.importer.payload.LocationWeatherData;
import de.fu_berlin.agdb.noaa_importer.core.DataGatherer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class NOAAImporter extends AWeatherImporter {

    private static final Logger logger = LogManager.getLogger(NOAAImporter.class);

    private static final int NUMBER_OF_THREADS = 1;
    @Override
    protected List<LocationWeatherData> getWeatherDataForLocations(List<LocationMetaData> locations) {

        DataGatherer dataGatherer = new DataGatherer(NUMBER_OF_THREADS, locations);
        try {
            return dataGatherer.gatherData();
        } catch (IOException e) {
            logger.error("IO error: " + e);
        }
        return null;
    }

    @Override
    protected long getServiceTimeout() {
        // 3 hours
        return 3*60*60*1000;
    }

}
