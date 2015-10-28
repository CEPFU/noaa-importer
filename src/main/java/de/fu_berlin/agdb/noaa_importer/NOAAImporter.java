package de.fu_berlin.agdb.noaa_importer;

import de.fu_berlin.agdb.importer.AWeatherImporter;
import de.fu_berlin.agdb.importer.payload.LocationMetaData;
import de.fu_berlin.agdb.importer.payload.LocationWeatherData;
import de.fu_berlin.agdb.noaa_importer.core.IWorkProvider;

import java.util.List;

public class NOAAImporter extends AWeatherImporter implements IWorkProvider {

    @Override
    protected List<LocationWeatherData> getWeatherDataForLocations(List<LocationMetaData> locations) {
        return null;
    }

    @Override
    protected long getServiceTimeout() {
        return 0;
    }

    @Override
    public LocationMetaData getWork() {
        return null;
    }

    @Override
    public void deliverResult(List<LocationWeatherData> dataForLocation) {

    }
}
