package com.psl.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class PopulateDB {

	private Bucket bucket;
	private Cluster cluster;
	List<String> datafiles;
	
	Logger logger = Logger.getLogger(PopulateDB.class);

	public PopulateDB() {
		super();
		// TODO Auto-generated constructor stub
		cluster = CouchbaseCluster.create();
	}

	public boolean initialize(String bucketname) {
		bucket = cluster.openBucket(bucketname);
		if (bucket != null)
			return true;
		else
			return false;
	}
	
	public boolean close() {
		return cluster.disconnect();
	}
	
	public void setDatafiles(String[] datafiles) {
		this.datafiles = new ArrayList<>();
		for (String file : datafiles) {
			this.datafiles.add(file);
		}
	}
	
	public void populate() throws IOException {
		String data = null;
		for (String filename : datafiles) {
			try(BufferedReader br = new BufferedReader(new FileReader(filename))) {
				while((data = br.readLine()) != null) {
					JsonObject obj = clean(data);
					String id = obj.get("dt").toString() + ":" + obj.getString("name");
					JsonDocument doc = JsonDocument.create(id, obj);
					JsonDocument res = bucket.upsert(doc);
					//System.out.println("Inserted #" + id);
					logger.debug("Inserted #" + id + ": " + res.toString());
				}
			}
		}
	}
	
	private JsonObject clean(String data) {
		// TODO Auto-generated method stub
		JsonObject jdata = JsonObject.create();
		JsonObject obj = JsonObject.fromJson(data);
		obj.removeKey("coord").removeKey("weather").removeKey("sys").removeKey("id").removeKey("cod").removeKey("base");
		jdata = ((JsonObject) obj.get("main")).removeKey("sea_level").removeKey("grnd_level");
		obj.removeKey("main");
		obj.put("main", jdata);
		return obj;
	}

}
