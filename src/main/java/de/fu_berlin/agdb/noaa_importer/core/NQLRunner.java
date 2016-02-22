package de.fu_berlin.agdb.noaa_importer.core;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class NQLRunner {

    private static final Logger logger = LogManager.getLogger(NQLRunner.class);

    private static final String BASE_URL = "http://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl";
    private static final String QUERY_FILE_PARAMETER = "?file=";
    private static final String QUERY_VAR_PARAMETER = "&var_APCP=on&var_GUST=on&var_PRATE=on&var_PRES=on&var_RH=on&var_SNOD=on&var_SUNSD=on&var_TCDC=on&var_TMAX=on&var_TMIN=on&var_TMP=on";
    private static final String QUERY_SUBREGION_PARAMETER = "&subregion=&leftlon=5.5&rightlon=16&toplat=55&bottomlat=47";
    private static final String QUERY_DIR_PARAMETER = "&dir=/gfs.";

    public File getNQLResponseFile(String fileName, String directory) throws IOException {
        logger.debug("Downloading " + fileName);
        File outputFile = new File(fileName);
        FileUtils.copyURLToFile(prepareURL(fileName, directory), outputFile, 5000, 5000);
        if (outputFile.length() > 0) {
            return outputFile;
        } else {
            return null;
        }
    }

    private URL prepareURL(String fileName, String directory) {
        String url = BASE_URL
                + QUERY_FILE_PARAMETER + fileName
                + QUERY_VAR_PARAMETER
                + QUERY_SUBREGION_PARAMETER
                + QUERY_DIR_PARAMETER + directory;
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            logger.error("Error creating URL '" + url + "' " + e.getMessage());
            return null;
        }
    }
}
