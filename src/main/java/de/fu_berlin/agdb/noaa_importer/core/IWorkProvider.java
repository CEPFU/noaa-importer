package de.fu_berlin.agdb.noaa_importer.core;

import de.fu_berlin.agdb.importer.payload.LocationMetaData;
import de.fu_berlin.agdb.importer.payload.LocationWeatherData;

import java.util.List;

public interface IWorkProvider {

    String getWork();

    void deliverResult(List<LocationWeatherData> dataForLocation);
}
