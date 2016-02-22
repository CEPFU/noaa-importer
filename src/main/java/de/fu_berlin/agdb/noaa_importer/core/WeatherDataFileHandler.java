package de.fu_berlin.agdb.noaa_importer.core;

import de.fu_berlin.agdb.importer.payload.DataType;
import de.fu_berlin.agdb.importer.payload.GridMetaData;
import de.fu_berlin.agdb.importer.payload.LocationWeatherData;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import ucar.ma2.Array;
import ucar.ma2.Index3D;
import ucar.ma2.Index4D;
import ucar.nc2.NetcdfFile;
import ucar.nc2.dataset.NetcdfDataset;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class WeatherDataFileHandler extends DataFileHandler {

    private static final Logger logger = LogManager.getLogger(WeatherDataFileHandler.class);

    public WeatherDataFileHandler(File file) {
        super(file);
    }

    @Override
    public List<LocationWeatherData> handleDataFile() {
        logger.debug("Analyzing weather data file " + getFile().getName());

        NetcdfFile netcdfFile = null;
        try {
            netcdfFile = NetcdfDataset.openFile(getFile().getAbsolutePath(), null);
            logger.debug("Opened file in netcdf " + getFile().getName());
            return process(netcdfFile);
        } catch (IOException e) {
            logger.error("Trying to open " + getFile().getName(), e);
        } finally {
            if (netcdfFile != null) {
                try {
                    netcdfFile.close();
                } catch (IOException e) {
                    logger.error("Trying to close " + getFile().getName(), e);
                }
            }
        }
        return null;
    }

    private List<LocationWeatherData> process(NetcdfFile netcdfFile) {
        try {
            Double forecastHour = netcdfFile.findVariable("time").read().getDouble(0);

            float[] latArray = (float[]) netcdfFile.findVariable("lat").read().copyTo1DJavaArray();
            float[] lonArray = (float[]) netcdfFile.findVariable("lon").read().copyTo1DJavaArray();

            int varHour = 0;
            if (forecastHour > 240) {
                varHour = 12;
            } else if (forecastHour % 2 == 1){
                varHour = 3;
            } else {
                varHour = 6;
            }

            Date date;

            Array cloudage = netcdfFile.findVariable("Total_cloud_cover_entire_atmosphere_" + varHour + "_Hour_Average").read(); // percent
            Array precipitationDepth = netcdfFile.findVariable("Total_precipitation_surface_" + varHour + "_Hour_Accumulation").read(); // kg/m^2
            Array temperatureHigh = netcdfFile.findVariable("Maximum_temperature_height_above_ground_" + varHour + "_Hour_Maximum").read(); // K
            Array temperatureLow = netcdfFile.findVariable("Minimum_temperature_height_above_ground_" + varHour + "_Hour_Minimum").read(); // K
            Array windChill = netcdfFile.findVariable("Temperature_maximum_wind").read(); // K
            Array windSpeed = netcdfFile.findVariable("Wind_speed_gust_surface").read(); // m/s
            Array atmosphereHumidity = netcdfFile.findVariable("Relative_humidity_sigma_layer").read(); // percent
            Array atmospherePressure = netcdfFile.findVariable("Pressure_surface").read(); // Pa
            Array temperature = netcdfFile.findVariable("Temperature_surface").read(); // K
            Array maximumWindSpeed = netcdfFile.findVariable("Wind_speed_gust_surface").read(); // m/s
            Array sunshineDuration = netcdfFile.findVariable("Sunshine_Duration_surface").read(); // s
            Array snowHeight = netcdfFile.findVariable("Snow_depth_surface").read(); // m

            List<LocationWeatherData> locationWeatherDataList = new ArrayList<>(1419);

            for (int latIndex = 0; latIndex < latArray.length; latIndex++) {
                for (int lonIndex = 0; lonIndex < lonArray.length; lonIndex++) {
                    GridMetaData gridMetaData = new GridMetaData(latArray[latIndex], lonArray[lonIndex]);

                    Index3D index3D = new Index3D(new int[] {0, latIndex, lonIndex});

                    LocationWeatherData locationWeatherData = new LocationWeatherData(gridMetaData, System.currentTimeMillis(), DataType.FORECAST);
                    locationWeatherData.setCloudage(cloudage.getDouble(index3D));
                    locationWeatherData.setPrecipitationDepth(precipitationDepth.getDouble(index3D));
                    locationWeatherData.setWindChill(windChill.getDouble(index3D));
                    locationWeatherData.setWindSpeed(windSpeed.getDouble(index3D));
                    locationWeatherData.setAtmosphereHumidity(atmosphereHumidity.getDouble(index3D));
                    locationWeatherData.setAtmospherePressure(atmospherePressure.getDouble(index3D));
                    locationWeatherData.setTemperature(temperature.getDouble(index3D));
                    locationWeatherData.setMaximumWindSpeed(maximumWindSpeed.getDouble(index3D));
                    locationWeatherData.setSunshineDuration(sunshineDuration.getDouble(index3D));
                    locationWeatherData.setSnowHeight(snowHeight.getDouble(index3D));

                    Index4D index4D = new Index4D(new int[] {0, 0, latIndex, lonIndex});

                    locationWeatherData.setTemperatureHigh(temperatureHigh.getDouble(index4D));
                    locationWeatherData.setTemperatureLow(temperatureLow.getDouble(index4D));

                    locationWeatherDataList.add(locationWeatherData);
                }
            }

            return locationWeatherDataList;
        } catch (IOException e) {
            logger.error("Trying to process " + getFile().getName(), e);
            return null;
        }

    }
}
