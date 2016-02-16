package de.fu_berlin.agdb.noaa_importer.core;

import de.fu_berlin.agdb.importer.payload.DataType;
import de.fu_berlin.agdb.importer.payload.GridMetaData;
import de.fu_berlin.agdb.importer.payload.LocationWeatherData;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.dataset.NetcdfDataset;

import java.io.IOException;
import java.sql.Date;

public class WeatherDataFileHandler extends DataFileHandler {

    private final static Logger logger = LogManager.getLogger(WeatherDataFileHandler.class);
    private INOAADataHandler noaaDataHandler;

    public WeatherDataFileHandler(IWeatherDataFileProvider weatherDataFileProvider, INOAADataHandler noaaDataHandler) {
        super (weatherDataFileProvider.getWeatherDataFile());
        this.noaaDataHandler = noaaDataHandler;
    }

    @Override
    public void handleDataFile() throws IOException {
        logger.debug("Analyzing weather data file " + getFile().getName());

        NetcdfFile netcdfFile = NetcdfDataset.openFile(getFile().getAbsolutePath(), null);
        logger.debug("Opened file in netcdf " + getFile().getName());

        Array latArray = netcdfFile.findVariable("lat").read();
        Array lonArray = netcdfFile.findVariable("lon").read();

        int[][] originAndSection = originAndSectionForCoordinates(55, 5.5, 47, 16, netcdfFile);

        GridMetaData gridMetaData = new GridMetaData();

        Date date;
        Array windChill                    = netcdfFile.findVariable("Temperature_maximum_wind").read(); // K
        Array windSpeed                    = netcdfFile.findVariable("Wind_speed_gust_surface").read(); // m/s
        Array atmosphereHumidity           = netcdfFile.findVariable("humidity_sigma_layer").read(); // percent
        Array atmospherePressure           = netcdfFile.findVariable("Pressure_surface").read(); // Pa
        Array temperature                  = netcdfFile.findVariable("Temperature_surface").read(); // K

        Array cloudage                     = netcdfFile.findVariable("Total_cloud_cover_entire_atmosphere_3_Hour_Average").read(); // percent
        Array minimumAirGroundTemperature  = netcdfFile.findVariable("Minimum_temperature_height_above_ground_3_Hour_Minimum").read(); // K
        Array maximumWindSpeed             = netcdfFile.findVariable("Wind_speed_gust_surface").read(); // m/s
        Array precipitationDepth           = netcdfFile.findVariable("Total_precipitation_surface_3_Hour_Accumulation").read(); // kg/m^2
        Array sunshineDuration             = netcdfFile.findVariable("Sunshine_Duration_surface").read(); // s
        Array snowHeight                   = netcdfFile.findVariable("Snow_depth_surface").read(); // m

        netcdfFile.close();

        // TODO save locationweatherdata for each grid

        LocationWeatherData locationWeatherData = new LocationWeatherData(gridMetaData, System.currentTimeMillis(), DataType.FORECAST);



        }

    // Raster Deutschland wie bei REGNIE
    // oben links 55°05'00.0"N+5°50'00.0"E
    // unten rechts 47°00'00.0"N+16°00'00.00"E

    // NOAA
    // oben links 55°00'00.0"N+5°30'00.0"E (55°N+5.5°E)
    // unten rechts 47°00'00.0"N+16°00'00.0"E (47°N+16°E)

    /**
     * Gets the two arrays origin and section needed for a subsection of the grid.
     *
     * @param originLat latitude in decimal degrees of the upper left corner
     * @param originLon longitude in decimal degrees of the upper left corner
     * @param targetLat latitude in decimal degrees of the lower right corner
     * @param targetLon longitude in decimal degrees of the lower right corner
     * @param file grid file
     * @return array of arrays containing the origin index and the section index
     */
    private int[][] originAndSectionForCoordinates(double originLat, double originLon, double targetLat, double targetLon, NetcdfFile file) {
        if (originLat < targetLat || originLon > targetLon) {
            throw new IllegalArgumentException("origin coordinates must be upper left corner");
        }
        int[] origin = {0,0};
        int[] section = {0,0};
        try {
            Array latData = file.findVariable("lat").read();
            float[] latArray = (float[]) latData.copyTo1DJavaArray();
            for (int latIndex = 0; latIndex < latArray.length; latIndex++) {
                if (latArray[latIndex] == originLat) {
                    origin[0] = latIndex;
                }
                else if (latArray[latIndex] == targetLat) {
                    section[0] = latIndex - origin[0];
                    break;
                }
            }
            Array lonData = file.findVariable("lon").read();
            float[] lonArray = (float[]) lonData.copyTo1DJavaArray();
            for (int lonIndex = 0; lonIndex < lonArray.length; lonIndex++) {
                if (lonArray[lonIndex] == originLon) {
                    origin[1] = lonIndex;
                }
                else if (lonArray[lonIndex] == targetLon) {
                    section[1] = lonIndex - origin[1];
                    break;
                }
            }
            return new int[][]{origin, section};
        } catch (IOException e) {
            logger.error("Error getting the index");
            return null;
        }
    }
}
