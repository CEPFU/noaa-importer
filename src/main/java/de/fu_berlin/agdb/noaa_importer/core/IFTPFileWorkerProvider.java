package de.fu_berlin.agdb.noaa_importer.core;

import org.apache.commons.net.ftp.FTPFile;

public interface IFTPFileWorkerProvider {
	public FTPFile getFTPFileToWorkWith();
}
