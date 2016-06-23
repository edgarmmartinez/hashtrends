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
import com.mongodb.DB;

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

	    System.out.println ("StreamRankQueue run");
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
	    TimedRankQueue<String> usernameRankQueue = new TimedRankQueue<String>(10);

	    // Do whatever needs to be done with messages
	    while (stopping == false) {
	      if (client.isDone()) {
	        System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
	        break;
	      }

	      String msg = queue.poll(5, TimeUnit.SECONDS);
	      
	      if (msg == null) {
	        System.out.println("Did not receive a message in 5 seconds");
	      } else {
	        JsonNode node = mapper.readTree(msg);

	        JsonNode        jText = node.get("text");
	        JsonNode        jLang = node.get("lang");
	        JsonNode        jUser = node.findValue("screen_name");
	        String[]        strArr = null;
	        
	        if (jText != null && jLang != null) {
	            if (jLang.asText().equals("en")) {
//	            if (jText != null) {
	                strArr = jText.asText().split("\\s+");
	                if (debug >= 1) System.out.println (jText.asText());
	                for (int i = 0; i < strArr.length; i++) {
	                    if (strArr[i].toCharArray() [0] == '#') {
	                        if (debug >= 1) System.out.println (strArr[i]);
	                        hashtagRankQueue.offer(strArr[i]);
	                    }
	                }
	                
	                if (jUser != null) {
	                    if (debug >= 2) System.out.println (jUser.asText());
	                    usernameRankQueue.offer(jUser.asText());
	                }
	                msgRead++;
	                
	                if (msgRead % 20 == 0) {
	                	if (debug > 0) {
		                    System.out.println ("-------------------------------------------");

		                    if (hashtagRankQueue.size() < 50) {
		                    	System.out.println("Hashtags in list: " + hashtagRankQueue.toString());
		                	}

		                    System.out.println(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(Calendar.getInstance().getTime()));
		                	System.out.println("Number of hashtags in list: " + hashtagRankQueue.size());
		                	System.out.println("Number of hashtags ranked: " + hashtagRankQueue.keyCount());
	                	}
	                	
	                	List<Map.Entry<String,Integer>> list = hashtagRankQueue.getRank();
	                	printRankList(list, 40);
	                }
	            }
	        }
	      }
	    }

	    client.stop();

	    // Print some stats
	    System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
	}
}
