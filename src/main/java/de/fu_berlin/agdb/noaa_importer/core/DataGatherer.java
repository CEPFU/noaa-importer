package de.fu_berlin.agdb.noaa_importer.core;

import de.fu_berlin.agdb.importer.payload.LocationMetaData;
import de.fu_berlin.agdb.importer.payload.LocationWeatherData;
import de.fu_berlin.agdb.importer.payload.StationMetaData;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.IOException;
import java.util.*;

public class DataGatherer implements IFTPFileWorkerProvider, INOAADataHandler {

    public static final String SERVER = "ftp.ncep.noaa.gov";
    public static final String DATA_DIRECTORY = "/pub/data/nccf/com/gfs/prod/gfs.2016021200/"; // TODO get the most recent directory

    private final int numberOfThreads;
    private List<FTPFile> ftpFiles;

    private List<LocationWeatherData> gatheredLocationWeatherData;
    private List<LocationMetaData> locations;
    private Map<Thread, FTPClient> ftpClients;

    public DataGatherer(int numberOfThreads, List<LocationMetaData> locations) {
        this.numberOfThreads = numberOfThreads;
        this.locations = locations;
        gatheredLocationWeatherData = new ArrayList<LocationWeatherData>();
        ftpClients = new HashMap<Thread, FTPClient>();
    }

    public List<LocationWeatherData> gatherData() throws IOException {
        FTPClient ftpClient = setupFTPClient(DATA_DIRECTORY);

        ftpFiles = new ArrayList<FTPFile>(Arrays.asList(ftpClient.listFiles()));

        List<Thread> threads = new ArrayList<Thread>();
        for (int threadNumber = 0; threadNumber < numberOfThreads; threadNumber++) {
            FTPClient threadFTPClient = setupFTPClient(DATA_DIRECTORY);
            Thread thread = new Thread(new FTPFileWorker(this, threadFTPClient, this));
            ftpClients.put(thread, threadFTPClient);
            threads.add(thread);
            thread.start();
        }

        waitForThreads(threads);

        shutDownFTPClient(ftpClient);
        return gatheredLocationWeatherData;
    }

    private void waitForThreads(List<Thread> threads) throws IOException {
        for (Thread thread : threads) {
            try {
                thread.join();
                shutDownFTPClient(ftpClients.get(thread));
            } catch (InterruptedException e) {
                waitForThreads(threads);
            }
        }
    }

    private FTPClient setupFTPClient(String directory) throws IOException{
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(SERVER);
        ftpClient.enterLocalPassiveMode(); // WORKAROUND https://stackoverflow.com/questions/2712967/apache-commons-net-ftpclient-and-listfiles
        ftpClient.login("anonymous", "");

        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

        ftpClient.changeWorkingDirectory(directory);
        return ftpClient;
    }

    private void shutDownFTPClient(FTPClient ftpClient) throws IOException {
        ftpClient.logout();
        ftpClient.disconnect();
    }

    @Override
    public synchronized FTPFile getFTPFileToWorkWith() {
        if (ftpFiles.size() > 0) {
            FTPFile file = ftpFiles.get(0);
            ftpFiles.remove(0);
            return file;
        }
        return null;
    }

    @Override
    public void addData(LocationWeatherData locationWeatherData) {
        gatheredLocationWeatherData.add(locationWeatherData);
    }

    @Override // TODO GridMetaData
    public StationMetaData getMetaDataForStation(long id) {
        for (LocationMetaData locationMetaData : locations) {
            if (locationMetaData instanceof StationMetaData) {
                StationMetaData stationMetaData = (StationMetaData) locationMetaData;
                if (stationMetaData.getStationId() == id) {
                    return stationMetaData;
                }
            }
        }
        return null;
    }
}
