package com.psl.test;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        Cluster cluster = CouchbaseCluster.create();
        Bucket bucket = cluster.openBucket();
        JsonObject user = JsonObject.empty()
        		.put("firstname", "Kshitij")
        		.put("lastname", "Turaskar")
        		.put("age", 26);
        JsonDocument doc = JsonDocument.create("19191", user);
        JsonDocument res = bucket.upsert(doc);
        System.out.println(res);
        cluster.disconnect();
    }
}
