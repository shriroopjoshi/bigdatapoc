package com.psl.test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * Defines starting point
 *
 */
public class App {

	static Logger logger;
	
	public static void main(String[] args) throws Exception {
		//System.out.println("Hello World!");
		logger = Logger.getLogger(App.class);


		 logger.info("Starting ETL");
		 System.out.println("Starting...");
		 Properties prop = new Properties();
		 InputStream inputStream = new FileInputStream("config.properties");
		 prop.load(inputStream);
		 String[] datafiles = prop.getProperty("datafiles").split(";");
		 String bucketName = prop.getProperty("bucketname");
		 String temp = prop.getProperty("datafiles");
		 inputStream.close();
		 logger.debug("bucketname: " + bucketName);
		 logger.debug("datafiles: " + temp);
		 
		 PopulateDB db = new PopulateDB();
		 db.initialize(bucketName);
		 db.setDatafiles(datafiles);
		 db.populate();
		 db.close();
		 
		 logger.info("ETL completed");
		 System.out.println("Completed!");
	}
}
