/**
 * 
 */
package org.isaseb.hashtrends;

import java.util.*;
import java.io.IOException;
import java.text.SimpleDateFormat;

// Twitter Hosebird client
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

//Imports for parsing JSON
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

//MongoDB imports
import com.mongodb.MongoClient;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;

import org.bson.Document;
import org.isaseb.utils.TimedRankQueue;
/**
 * @author edgar
 *
 */
public class HashTrends {

	/**
	 * @param args
	 */

    static void printRankList (List<Map.Entry<String,Integer>> list, int topN) {
    	for (int i = 0; i < topN && i < list.size(); i++) {
    		System.out.println (list.get(i).getKey() + " : " + list.get(i).getValue());
    	}
    }
    
	public static void main(String[] args) throws InterruptedException, IOException {
		boolean stopping = false;
		long msgRead = 0;
		
		// TODO: use proper java arg parsing
		String consumerKey = args[0];
		String consumerSecret = args[1];
		String token = args[2];
		String secret = args[3];
		int trendSec = Integer.parseInt(args[4]);
		int debug = Integer.parseInt(args[5]);
		
		// TODO: use configuration file for most arguments
		
	    // Create an appropriately sized blocking queue
	    BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

	    // Define our endpoint: By default, delimited=length is set (we need this for our processor)
	    // and stall warnings are on.
	    StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
	    endpoint.stallWarnings(false);

	    Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
	    //Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);

	    // Create a new BasicClient. By default gzip is enabled.
	    BasicClient client = new ClientBuilder()
	            .name("hashtrends")
	            .hosts(Constants.STREAM_HOST)
	            .endpoint(endpoint)
	            .authentication(auth)
	            .processor(new StringDelimitedProcessor(queue))
	            .build();

	    // Establish a connection
    	client.connect();

	    ObjectMapper mapper = new ObjectMapper();
	    TimedRankQueue<String> hashtagRankQueue = new TimedRankQueue<String>(trendSec);
/*	    TimedRankQueue<Document> hashDocRankQueue = new TimedRankQueue<Document>(trendSec);*/
	    TimedRankQueue<String> usernameRankQueue = new TimedRankQueue<String>(10);

        Document hashtagQueueFilter = new Document ("info", "lastQueue");

	    // MongoDB setup
	    MongoClient mongoClient = new MongoClient("localhost");
        MongoDatabase database = mongoClient.getDatabase("hashtrendsDB");
        
        // get a handle to the collection with the hashtags in it
        MongoCollection<Document> hashranksColl = database.getCollection("hashranks");
        MongoCollection<Document> hashtagQueueColl = database.getCollection("hashtagQueue");
/*        MongoCollection<Document> hashDocQueueColl = database.getCollection("hashDocQueue");*/
        ListIndexesIterable<Document> indexDocsIt = hashranksColl.listIndexes();
        
        // drop all the data in it. TODO: decide if it's better to start from scratch
        //hashtagCollection.drop();
        
        // TODO: parameters for expiration should be either in config file, passed in, or both
        hashranksColl.createIndex(new Document ("creationDate",1), new IndexOptions().expireAfter((long)4, TimeUnit.HOURS));
        // TODO: statement above must be changed to: "Instead use the collMod database command in conjunction with the index collection flag" to avoid exceptions of creating already existing index
        if (debug>=1) {
            for (Document idx : indexDocsIt) {
            	System.out.println ("Index: " + idx.toJson());
            }
        }
        
        // Populate persisted hashtag ranks
        // TODO: Only populate them if they were created within the last X minutes ("trendSec" seconds?)
        try {
//        	Document	initDoc = hashtagCollection.find().sort(new Document("_id", -1)).first();
        	Document	initDoc = hashtagQueueColl.find(hashtagQueueFilter).first();
        	if (debug >= 1) {
                System.out.println("queueContents list: " + initDoc.get("queueContents").toString());
        	}

        	for (String hashStr : (List<String>) initDoc.get("queueContents")) {
        		hashtagRankQueue.add(hashStr);
        	}
        	
/*        	for (Document hashDoc : (List<Document>) initDoc.get("queueContents")) {
        		hashDocRankQueue.add(hashDoc);
        	}
*/        } catch (NullPointerException e) {
    		System.out.println("Empty database: " + e.getStackTrace());
    		//Initialize queue database
    		hashtagQueueColl.insertOne(hashtagQueueFilter);
        }

	    // Do whatever needs to be done with messages
        // TODO: capture Ctrl-C (TERM signal) to exit while loop gracefully
	    while (stopping == false) {
		    if (client.isDone()) {
		      System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
		      break;
		    }
	
		    String msg = queue.poll(5, TimeUnit.SECONDS);
		      
		    if (msg == null) {
		      System.out.println("Did not receive a message in 5 seconds");
		      continue;
		    }
		    
	        JsonNode node = mapper.readTree(msg);

	        JsonNode        jText = node.get("text");

	        //TODO: support choosing search params (like "lang") live
//	          if (jLang.asText().equals("en") == false) {
	        if (jText == null) {
	        	continue;
	        }
          
	        JsonNode        jLang = node.get("lang");
	        JsonNode        jUser = node.findValue("screen_name");
	        String[]        strArr = null;
            HashSet<String> strSet = new HashSet<String>();
	        
            strArr = jText.asText().split("\\s+");
            if (debug >= 2)
            	System.out.println (jText.asText());
            
            //extract message hashtags and add to rank queue
            strSet.clear();
            for (int i = 0; i < strArr.length; i++) {
                if (strArr[i].toCharArray() [0] == '#') {
                	String cleanHashString = strArr[i].replaceAll("[#.,]", "");
                	// Repeated hashtag in one message should count as one
                    if (strSet.add(cleanHashString)) {
                    	if (debug >= 2)
                    		System.out.println (cleanHashString);
                    hashtagRankQueue.offer(cleanHashString);
                    } else {
                    	if (debug >= 2)
                    		System.out.println (cleanHashString + " is repeated in this message");
                    }
                }
            }
            
            // TODO: support rank stats of other information: geo-location
            if (jUser != null) {
                if (debug >= 2) System.out.println (jUser.asText());
                usernameRankQueue.offer(jUser.asText());
            }
            msgRead++;
            
        	List<Map.Entry<String,Integer>> hashtagList = hashtagRankQueue.getRank();

        	if (msgRead % 20 == 0) {
            	Document doc = new Document("creationDate", new Date(System.currentTimeMillis()));
            	List<Document> list = new ArrayList<Document>(40);

            	// TODO: The number of entries to store should be configurable
            	for (int i = 0; i < 40 && i < hashtagList.size(); i++) {
            		String cleanHashStr = hashtagList.get(i).getKey().replaceAll("[#.]", "");
            		if (!cleanHashStr.equals("")) {
                		Document entry = new Document("hash", cleanHashStr);
                		entry.append ("rank", hashtagList.get(i).getValue());
                		list.add(entry);
            		}
            	}
            	
            	doc.append("hashrankList", list);
            	
            	// TODO: Support aging out information after days, weeks, or maybe months, while saving stats
                hashranksColl.insertOne(doc);
                
                // TODO: Find better way than changing to array then to list
                hashtagQueueColl.findOneAndReplace(hashtagQueueFilter,
                		new Document (hashtagQueueFilter).append("queueContents", Arrays.asList(hashtagRankQueue.toArray())));

                if (debug >= 1) {
                    System.out.println ("-------------------------------------------");

                    if (hashtagRankQueue.size() < 50) {
                    	System.out.println("Hashtags in list: " + hashtagRankQueue.toString());
                	}

                    System.out.println(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(Calendar.getInstance().getTime()));
                	System.out.println("Number of hashtags in list: " + hashtagRankQueue.size());
                	System.out.println("Number of hashtags ranked: " + hashtagRankQueue.keyCount());
                	
                	printRankList(hashtagList, 40);
            	}
            }
	    }

	    mongoClient.close();
	    client.stop();

	    // Print some stats
	    System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
	}
}
