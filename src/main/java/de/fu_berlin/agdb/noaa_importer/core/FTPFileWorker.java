package de.fu_berlin.agdb.noaa_importer.core;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;

public class FTPFileWorker implements Runnable {

    private static final Logger logger = LogManager.getLogger(FTPFileWorker.class);

    private static final int FORECAST_HOURS = 3; // TODO in properties
    private static final String GRID_SIZE = "0p25"; // TODO in properties

    private IFTPFileWorkerProvider workerProvider;
    private FTPClient ftpClient;

    private INOAADataHandler noaaDataHandler;

    public FTPFileWorker(IFTPFileWorkerProvider workerProvider, FTPClient ftpClient, INOAADataHandler noaaDataHandler) {
        this.workerProvider = workerProvider;
        this.ftpClient = ftpClient;
        this.noaaDataHandler = noaaDataHandler;
    }

    public void run() {
        String fileNameRegex = fileNameRegex(); // TODO use it!

        FTPFile ftpFileToWorkWith;
        while ((ftpFileToWorkWith = workerProvider.getFTPFileToWorkWith()) != null) {
            if (ftpFileToWorkWith.isFile() && ftpFileToWorkWith.getName().contains("gfs.t00z.pgrb2.0p25.f003")) { // TODO fileNameRegex
                File loadedFile = new File(ftpFileToWorkWith.getName());
                try {
                    if (retrieveFtpFile(ftpClient, ftpFileToWorkWith, loadedFile)) {
                        logger.debug("Downloaded " + ftpFileToWorkWith.getName());
                        GribDataHandler gribDataHandler = new GribDataHandler(loadedFile);
                        WeatherDataFileHandler weatherDataFileHandler = new WeatherDataFileHandler(gribDataHandler, noaaDataHandler);
                        weatherDataFileHandler.handleDataFile();
                        logger.debug("Handled " + ftpFileToWorkWith.getName());
                        // TODO delete temporary files as well
                        loadedFile.delete();
                        logger.debug("Deleted " + ftpFileToWorkWith.getName());
                    } else {
                        logger.debug("Failed loading " + ftpFileToWorkWith.getName());
                    }
                } catch (IOException e) {
                    logger.error(e);
                }
            }
        }
    }

    private boolean retrieveFtpFile(FTPClient ftpClient, FTPFile ftpFile, File file) throws IOException {
        // TODO check if file exists with same size and don't download again
        logger.debug("Downloading ftp file " + ftpFile.getName());
        if (ftpFile.getSize() != file.length()) {
            OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
            boolean success = ftpClient.retrieveFile(ftpFile.getName(), outputStream);
            outputStream.close();
            return success;
        } else {
            return true;
        }
    }

    private String fileNameRegex() {
        if (FORECAST_HOURS % 3 != 0) {
            throw new IllegalArgumentException("Forecast hours not a multiple of 3");
        }
        String fileNameRegex = "gfs.t(?:00|06|12|18).pgrb2.";
        fileNameRegex += GRID_SIZE;
        fileNameRegex += ".f";

        int forecastHours = FORECAST_HOURS;
        String foreCastHoursRegex = "(?:";
        while (forecastHours > 0) {
            foreCastHoursRegex += String.format("%03d", forecastHours);
            foreCastHoursRegex += "|";
            forecastHours = forecastHours - 3;
        }
        foreCastHoursRegex = foreCastHoursRegex.substring(0, foreCastHoursRegex.length() - 1);
        foreCastHoursRegex += ")";
        return fileNameRegex + foreCastHoursRegex;
    }
}
