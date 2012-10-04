package com.twitter;

import java.util.List;
import java.util.Properties;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

/*-----Not using for now-----------------*/
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.sql.*;
import java.util.Arrays;
import java.io.*;
import java.util.logging.*;
//----------------------------------------

//------For json parsing------------------
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.ParseException;
//----------------------------------------

//------For the connection of Mongodb-----
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
/*-----Not using for now-----------------*/
import com.mongodb.DBCursor;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import com.twitter.neo4j;

import org.bson.io.BasicOutputBuffer;
import org.bson.types.ObjectId;
import org.bson.util.*;
import org.python.util.PythonInterpreter;
//-----------------------------------------

//------using the twitter4j lib------------
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Tweet;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
/*-----Not using for now-----------------*/
import twitter4j.auth.AccessToken;
import twitter4j.internal.http.HttpParameter;
import twitter4j.json.DataObjectFactory;

import org.apache.log4j.Logger;
//-----------------------------------------

@SuppressWarnings("unused")
public class Processor {
	/**
	#	 * Log4j logger
	#	 */
	
	Mongo mongo;
	DB db;
	
	volatile DBCollection collat;//all_tweets
	volatile DBCollection collrd;//raw_data
	volatile DBCollection collta;//tweet_appearence
	volatile DBCollection collslot;//current_slot
	volatile DBCollection collsearch;//search_terms
	volatile DBCollection colltree;//tree_nodes
	volatile BasicDBObject index_user_rate;
	volatile BasicDBObject index_search_terms;
	volatile BasicDBObject index_tweet_appearence;
	volatile BasicDBObject index_all_tweets;
	volatile BasicDBObject index_over_all;
	volatile DBCollection collrate;
	volatile DBCollection collsr;
	volatile BasicDBObject index_sr;
	volatile BasicDBObject index_tree;
	static volatile int[][] multD =null;
	Logger log4j = Logger.getLogger("twitterjava.log");
	//static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder(); // or "ISO-8859-1" for ISO Latin 1
	public int current_slot_index;// index of current time slot
    public long current_slot_start_time;// starting time of current time slot
    public long frame_time;// total frame time
    public int threshold;// threshold for distance function
    public int depth;// depth to look in tree
    public long num_of_slots;
    public double slot_time_millis;// time of a single time slot
    static volatile long tweet_id_index;
    //static Long MaximumInsertSize=160L;
    //public neo4j myNeoInstance = new neo4j();
	//myNeoInstance.createDb();
	
	public Processor(Mongo mongo,DB db ,double slot_time_millis , long frame_time , int threshold , int depth , long num_of_slots) 
	{
		
		
		this.mongo=mongo;
		this.db=db;
		this.collat=db.getCollection("all_tweets");
		this.collrd=db.getCollection("raw_data");
		this.collslot=db.getCollection("current_slot");
		this.collsearch=db.getCollection("search_terms");
		this.current_slot_index = 0;
		this.current_slot_start_time = 0;
		this.collta=db.getCollection("tweet_appearance");
		this.collrate=db.getCollection("user_rate");
		this.colltree = db.getCollection("tree_nodes");
		this.collsr = db.getCollection("search_results");
		this.slot_time_millis = slot_time_millis;
		this.threshold = threshold;
		this.depth = depth;
		this.frame_time = frame_time;
		this.num_of_slots = num_of_slots;
		this.index_all_tweets = new BasicDBObject("id" , 1);
		this.index_search_terms = new BasicDBObject("search_term" , 1);
		this.index_over_all = new BasicDBObject("over_all" , 1);
		this.index_tweet_appearence = new BasicDBObject("tweet_id" , 1);
		this.index_user_rate = new BasicDBObject("user_id" , 1);
		this.index_sr = new BasicDBObject("searchword" , 1);
		this.index_tree = new BasicDBObject("parent",1);
		//this.index_tree.put("son", 1);
		log4j.info("ensuring uniqe index for tweet id in all_tweets collection");
		this.collat.ensureIndex(this.index_all_tweets , "tweet_id" , true);
		log4j.info("ensuring uniqe index for user id in user_rate collection");
		this.collrate.ensureIndex(this.index_user_rate, "user_id", true);
		log4j.info("ensuring uniqe index for search term in search terms collection");
		this.collsearch.ensureIndex(this.index_search_terms, "search_term", true);
		log4j.info("ensuring non uniqe index for over_all in search terms collection");
		this.collsearch.ensureIndex(this.index_over_all, "over_all", false);
		log4j.info("ensuring non uniqe index for id in tweet_appearence collection");
		this.collta.ensureIndex(this.index_tweet_appearence, "tweet_id", true);
		log4j.info("ensuring uniqe index for searchword in search_results collection");
		this.collsr.ensureIndex(this.index_sr , "searchword" , true);
		//this.tweet_id_index = 0;
		log4j.info("ensuring uniqe index for parent and son in tree nodes collection");
		colltree.ensureIndex(this.index_tree , "parentson" , true);
		
		//this.myNeoInstance.createDb();
		
		
	}
	
	//----This function gets as a parameter a list of terms----
	//---------the function returns a list of tweet ids from collection search_results-----------
	//----------------------------------------------------------------------------------------
	public LinkedList <String> get_tweets(LinkedList <String> search_terms)
	{
		log4j.info("starting function get_tweets");
		LinkedList<String> result = new LinkedList <String>();
		Iterator<String> terms = search_terms.iterator();
		long curr_time = System.currentTimeMillis();
		long min_time = curr_time - this.frame_time;
		int count_all = 0;
		while (terms.hasNext())
		{
			int count = 0;
			String term = terms.next();
			DBObject st = new BasicDBObject();
			try{
				st.put("searchword", term); 
				DBObject obj = this.collsr.findOne(st);
				String[] tweets_plus_time = obj.get("tweets").toString().split(",");
				String new_string = "";
				for (int i=0;i<tweets_plus_time.length-1;i+=2)
				{
					if (Long.parseLong(tweets_plus_time[i+1]) >= min_time)
					{
						result.add(tweets_plus_time[i]);
						count++;
						if (new_string == "")
						{
							new_string += tweets_plus_time[i] + "," + tweets_plus_time[i+1];
							//count++;
						}
						else
						{
							new_string += "," + tweets_plus_time[i] + "," + tweets_plus_time[i+1];
						}
					}
				}
				count_all += count;
				log4j.info(count + " tweets for term: " + term);
				obj.put("tweets", new_string);
			}
			catch (NullPointerException e){
				log4j.info("search_term: " + term + ", is not in collection search_results");
			}
			
		}
		log4j.info("over_all there are " + count_all + " tweets to compare!!!");
		log4j.info("ending function get_tweets");
		return result;
	}
	
	//----This function gets as a parameter the input terms list from user----
	//---------the function returns a list of tweet_ids-----------
	//----------------------------------------------------------------------------------------
	public LinkedList<String> get_final_tweet_ids(LinkedList <String> a){
		a = neo4j.getallsearchterms(a, depth, log4j);
		LinkedList<String> b = this.get_tweets(a);
		return b;
	}
	
	//----This function getting nodes from mongo's collection tree_nodes----
	//---------the function insert nodes to neo4j and deletes it from mongo-----------
	//----------------------------------------------------------------------------------------
	public void update_tree()
	{
		while (true)
		{
			DBCursor cursor = this.colltree.find();
			while (cursor.count() < 1){
				try {
					log4j.info("there is no nodes to process at the moment, going to sleep for 10 seconds");
					//log4j.info("update_all_tweets is going to sleep fo 0.25 minute, " + countelements.toString() + " elements moved so far, no docs currently in raw data");
					Thread.currentThread();
					Thread.sleep(1000*10);
					//System.out.println("update_all_tweets woke up ");
					log4j.info("update_tree woke up, continues");
					cursor = this.colltree.find();
				} catch (InterruptedException e) {
					log4j.error("InterruptedException caught, at update_all_tweets");
					e.printStackTrace();
					log4j.error(e);
				}
			}
			try
			{
				while (cursor.hasNext())
				{
					DBObject tr = cursor.next();
					try
					{
						String parent = tr.get("parent").toString();
						String[] sons = tr.get("son").toString().split(",");
						this.colltree.remove(tr);
						//log4j.info("adding nodes to neo4j, parent is:  " + parent + "  ,  and son is:  " + son);
						neo4j.addNode(parent, sons , this.log4j);
						
					}
					catch (Exception e)
					{
						//log4j.error(e);
						System.out.println(e);
						this.colltree.remove(tr);
					}
					
				}
			}
			catch (MongoException e)
			{
				log4j.error(e);
			}
			
		}
	}
	
	
	//----This function gets the raw json from raw_data collection----
	//---------the function separate the tweets and inserts it to all_tweets collection and calling all other data processing methods-----------
	//----------------------------------------------------------------------------------------
	public void update_all_tweets(final double num_of_slots , final double max_time_frame_hours)throws MongoException, ParseException
	{
		log4j.info("starting update_all_tweets function");
		String res = new String();
		Integer countelements = 0;
		while(true)
		{
			DBCursor cursor = this.collrd.find();
			while (this.collrd.count() < 1){
				try {
					log4j.info("there is no raw data at the moment, going to sleep for 1 seconds");
					Thread.currentThread();
					Thread.sleep(1000*1);
					log4j.info("woke up, continues");
					cursor = this.collrd.find();
				} catch (InterruptedException e) {
					log4j.error("InterruptedException caught, at update_all_tweets");
					log4j.error(e);
				}
			}
			cursor = this.collrd.find();
			try{
			    while (cursor.hasNext())
			    {
			    	DBObject currdoc = cursor.next();
			    	log4j.info("getting a document from the raw data db");
			    	Object results=currdoc.get("results");
			    	try{
			    		res = results.toString();
			    	}
			    	catch (NullPointerException e){
			    		res = "";
			    	}
			    	Object obj=JSONValue.parse(res);
			    	log4j.info("making an array from the jsons tweets");
			    	JSONArray array=(JSONArray)obj;
			    	//JSONParser parser = new JSONParser();
			    	try{
			    		if (res != ""){
				    		@SuppressWarnings("rawtypes")
							Iterator iterArray = array.iterator();
				    		log4j.info("iterating over array tweets");
				    		try
				    		{
				    			while (iterArray.hasNext()){
					    			Object current = iterArray.next();
					    			final DBObject dbObject = (DBObject)JSON.parse(current.toString());
					    			countelements++;
					    			//System.out.println("element number" + countelements.toString());
					    			dbObject.put("max_id", currdoc.get("max_id"));
					    			dbObject.put("query", currdoc.get("query"));
					    			dbObject.put("query_time", currdoc.get("query_time"));
					    			dbObject.put("query_time_string", currdoc.get("query_time_string"));
					    			dbObject.put("text", "@" + dbObject.get("from_user").toString() + ": " + dbObject.get("text").toString());
					    			log4j.info("inserting tweet id: " + dbObject.get("id").toString());
					    			collat.insert(dbObject);
					    			//collrd.findAndRemove(currdoc);
					    			//log4j.info("calling function update_search_terms");
					    			//final String text = "@" + dbObject.get("from_user").toString() + ": " + dbObject.get("text").toString();
					    			
					    			
					    			Thread t10=new Thread(new Runnable(){
					    				public void run(){
					    					UpdateTweetCounterId(Long.parseLong(dbObject.get("id").toString()));
					    				}
					    			});
					    			
					    			Thread t11=new Thread(new Runnable(){
					    				public void run(){
					    					update_search_terms(dbObject.get("text").toString()  , num_of_slots , max_time_frame_hours , dbObject.get("query").toString());
					    				}
					    			});
					    			
					    			Thread t12=new Thread(new Runnable(){
					    				public void run(){
					    					rate_user(Long.parseLong(dbObject.get("from_user_id").toString()) , dbObject.get("from_user").toString() , max_time_frame_hours);
					    					//UpdateUserRate((long)num_of_slots,slot_time_millis,Long.parseLong(dbObject.get("from_user_id").toString()) , dbObject.get("from_user").toString() ,(long)0);
					    				}
					    			});
					    			
					    			Thread t13=new Thread(new Runnable(){
					    				public void run(){
					    					String quer = dbObject.get("query").toString();
					    					quer = quer.replaceAll("%40", "@");
					    					quer = quer.replaceAll("%23", "#");
					    					long id = (long) (Double.parseDouble(dbObject.get("query_time").toString()) * 1000);
					    					String idplus = dbObject.get("id").toString() + "," + id;
					    					SearchResultId(quer , idplus );
					    				}
					    			});
					    			t10.start();
					    			t11.start();
					    			t12.start();
					    			t13.start();
					    			try {
					    			      log4j.info("Waiting for threads to finish.");
					    			      t10.join();
					    			      t11.join();
					    			      t12.join();
					    			      t13.join();
					    			    } catch (InterruptedException e) {
					    			    	log4j.error("Main thread (update_all_tweets) Interrupted");
					    			    }
					    			
					    			
					    			
					    		}
				    		}
				    		catch (Exception e)
				    		{
				    			log4j.error(e);
				    			e.printStackTrace();
				    		}
				    	
				    			
				    	}
			    	}
			    	catch (NullPointerException e){
			    		log4j.error(e);
			    		log4j.info("NullPointerException caught, at update_all_tweets");
			    	}
			    	log4j.info("removing processed document from raw_data collection");	
			    	try{
			    		this.collrd.remove(currdoc);
			    	} catch (Exception e){
			    		log4j.debug(e);
			    	}
		    	
		    }
			} catch (MongoException e){
				log4j.error(e);
			}
	}
	    
	}
	
	
	//----This function getting search terms and inserts/updates counter with time slots handling----
	//---------the function making shore the data is always up to date-----------
	//----------------------------------------------------------------------------------------
	@SuppressWarnings("deprecation")
	public void update_search_terms(String text , double num_of_slots , double max_time_frame_hours , String query) throws MongoException{
		log4j.info("starting function update_search_terms, num_of_slots = " + num_of_slots + ", max_time_frame_hours = " + max_time_frame_hours
				 + ", query = " + query);
		String[] textarray = text.split(" "); // split tweet text into a words array
		log4j.info("split tweet text into a word array");
		BasicDBObject objterm = new BasicDBObject();
		DBObject objtoupd = new BasicDBObject();
		DBObject update = new BasicDBObject();
		DBObject curr_slot = new BasicDBObject();
		
		log4j.info("starting function update_search_terms");
		curr_slot = this.collslot.findOne();
		this.current_slot_start_time = (long)(Double.parseDouble((curr_slot.get("slot_start_time").toString())));
		Date resultdate = new Date(this.current_slot_start_time);
		log4j.info("current_slot_start_time is : " + resultdate.toLocaleString());
		this.current_slot_index = Integer.parseInt(curr_slot.get("current_slot").toString());
		log4j.info("current time slot is  : " + this.current_slot_index);
		long difference = System.currentTimeMillis() - this.current_slot_start_time;
		
		if (difference > this.slot_time_millis){
			this.current_slot_start_time += (long)this.slot_time_millis;
			this.current_slot_index = (int) ((this.current_slot_index+1) % num_of_slots);
			log4j.info("new slot time has come, new slot is slot number " +  this.current_slot_index);
			curr_slot.put("current_slot", this.current_slot_index);
			curr_slot.put("slot_start_time", this.current_slot_start_time);
			curr_slot.put("slot_start_time_string", new Date(this.current_slot_start_time).toLocaleString());
			log4j.info("updating new current slot time and number in db");
			this.collslot.save(curr_slot);
			
			DBCursor terms = this.collsearch.find();
			log4j.info("1");
			while (terms.hasNext()){
				try{
					DBObject term = terms.next();
					if (term.get("search_term") != null){
						objtoupd.put("search_term", term.get("search_term"));
						term.put("slot" + this.current_slot_index, 0);
						term.put("current_slot", this.current_slot_index);
						term.put("over_all", Integer.parseInt(term.get("over_all").toString()) - Integer.parseInt(term.get("slot" + this.current_slot_index).toString()));
						
						this.collsearch.save(term);
					}
				} catch (NullPointerException e){
					e.printStackTrace();
					log4j.info(e);
				}
				
			}
			}
		log4j.info("going over the tweet text");
		query = query.replaceAll("%40", "@");
		query = query.replaceAll("%23", "#");
		for (int i=0;i<textarray.length;i++){ // loop over the words of the tweet
			if (textarray[i].trim().startsWith("@") || textarray[i].trim().startsWith("#")) { 
				String thisterm = textarray[i].trim();
				String[] no_ddot = thisterm.split("[:,., ,;,\n]");
				//no_ddot = no_ddot[0].split(",");
				//no_ddot = no_ddot[0].split(".");
				thisterm = no_ddot[0];
				thisterm = thisterm.replaceAll("%40", "@");
				thisterm = thisterm.replaceAll("%23", "#");
				if (thisterm.length() > 1){
					//if (isPureAscii(thisterm)){
					log4j.info("search word: " +  thisterm);
					objterm.put("search_term", thisterm); // object to find the search word in collection
					log4j.info("inserting tree nodes to mongodb");
					if (String.valueOf(query) != String.valueOf(thisterm)){
						DBObject nodes = new BasicDBObject();
						nodes.put("parent", query);
						try
						{
							nodes = colltree.findOne(nodes);
							nodes.put("son", nodes.get("son").toString() + "," + thisterm);
						}
						catch (NullPointerException e)
						{
							nodes = new BasicDBObject();
							nodes.put("parent", query);
							nodes.put("son", thisterm);
						}
						//nodes.put("son", thisterm);
						this.colltree.save(nodes);
						//neo4j.addNode(query, thisterm, log4j);
					}
					//objtoupd = collsearch.findOne(objterm); // find the search word in collection
					try{
						DBObject term = this.collsearch.findOne(objterm);
						term.put("over_all", Integer.parseInt(term.get("over_all").toString()) + 1);
						term.put("slot" + current_slot_index, Integer.parseInt(term.get("slot" + this.current_slot_index).toString()) + 1);
						//term.put("current_slot_start_time_millis", current_slot_start_time);
						log4j.info("updating counter in current slot for word: " + thisterm);
						this.collsearch.update(objterm, term);
					}
					catch (NullPointerException e){
						//e.printStackTrace();
						log4j.info(thisterm + " is not yet in collection , inserting it");
						DBObject newline = new BasicDBObject();
						newline.put("search_term", thisterm);
						newline.put("over_all", 1);
						newline.put("max_id", 0);
						newline.put("current_slot", current_slot_index);
						newline.put("current_slot_start_time_millis", current_slot_start_time);
						for (int j=0;j<num_of_slots;j++){
							if (j == current_slot_index){
								newline.put("slot" + current_slot_index, 1);
							}
							else{
								newline.put("slot" + j, 0);
							}
						}
						
						//newline.put("percentage", 100);
						this.collsearch.insert(newline);
						
					}
					//}
				}
				}
			}
		log4j.info("end update_search_terms");
		
	}
	
	//----This function getting user id and inserts/updates counter with time slots handling----
	//----------------------------------------------------------------------------------------
	public void rate_user(long user_id , String user_name , double max_time_frame_hours) throws MongoException{
		//log4j.info("starting function rate");
		//BasicDBObject objind = new BasicDBObject();
		BasicDBObject objterm = new BasicDBObject();
		DBObject objtoupd = new BasicDBObject();
		DBObject update = new BasicDBObject();
		log4j.info("starting function rate user for : user_name = " + user_name + ", user_id = " + user_id +
				", max_time_frame_hours = " + max_time_frame_hours);
		//objind.put("user_id", 1); // an object to ensure a unique index of search term
		//log4j.info("ensuring unique index user_id in user_rate collection");
		//this.collrate.ensureIndex(objind, "user_id", true); // ensure a unique index of search term
		try{
			objterm.put("user_id", user_id);
			DBObject term = this.collrate.findOne(objterm);
			int previous_slot = (Integer) term.get("current_slot");
			double delta = (System.currentTimeMillis() - (Long) term.get("current_slot_start_time_millis")) / this.slot_time_millis ;
			if (delta < 1){
				//term.put("over_all", Integer.parseInt(term.get("over_all").toString()) + 1);
				term.put("slot" + current_slot_index, Integer.parseInt(term.get("slot" + this.current_slot_index).toString()) + 1);
				log4j.info("updating counter in current slot for userid: " + user_id + " user_name : " + user_name);
				this.collrate.update(objterm, term);
			}
			else if(delta < this.num_of_slots){
				for (long h = 0;h<delta;h++){
					long slot = (long) ((long) (this.current_slot_index + num_of_slots - h)% num_of_slots);
					
					if (h == 0){
						term.put("slot" + slot, 1);
						//term.put("over_all", Integer.parseInt(term.get("over_all").toString())  + 1 - Integer.parseInt(term.get("slot" + slot).toString()));
					}
					else{
						//term.put("over_all", Integer.parseInt(term.get("over_all").toString()) - Integer.parseInt(term.get("slot" + slot).toString()));
						term.put("slot" + slot, 0);
					}
					
				}
				term.put("current_slot", this.current_slot_index);
				term.put("current_slot_start_time_millis" , this.current_slot_start_time);
				log4j.info("updating all slots needed: " + user_id + " user_name : " + user_name);
				this.collrate.update(objterm, term);
			}
			else{
				//term.put("over_all", 1);
				//newline.put("max_id", 0);
				term.put("current_slot", current_slot_index);
				term.put("current_slot_start_time_millis", current_slot_start_time);
				for (int j=0;j<num_of_slots;j++){
					if (j == current_slot_index){
						term.put("slot" + current_slot_index, 1);
					}
					else{
						term.put("slot" + j, 0);
					}
				}
			}
			
			
		}
		catch (NullPointerException e){
			//e.printStackTrace();
			log4j.info(user_name + " is not yet in collection , inserting it");
			DBObject newline = new BasicDBObject();
			newline.put("user_id", user_id);
			newline.put("user_name", user_name);
			//newline.put("over_all", 1);
			//newline.put("max_id", 0);
			newline.put("current_slot", current_slot_index);
			newline.put("current_slot_start_time_millis", current_slot_start_time);
			for (int j=0;j<num_of_slots;j++){
				if (j == current_slot_index){
					newline.put("slot" + current_slot_index, 1);
				}
				else{
					newline.put("slot" + j, 0);
				}
			}
			
			//newline.put("percentage", 100);
			this.collrate.insert(newline);
			
		}
			
		log4j.info("end rate_user");
		
	}
	
	//----This function getting tweet id and inserts/updates counter ----
	public void UpdateTweetCounterId(long tweet_id)
	{
		log4j.info("starting function for tweet id : " + tweet_id);
		BasicDBObject objind = new BasicDBObject();
		try
     	{
     		BasicDBObject query5 = new BasicDBObject();
     		query5.put("tweet_id",tweet_id);
     		DBObject obj = this.collta.findOne(query5);
     		obj.put("counter",Long.parseLong(obj.get("counter").toString())+1);
     	}
     	catch (NullPointerException e)
     	{
     		BasicDBObject doc = new BasicDBObject();
     		doc.put("tweet_id",tweet_id);//the same as Id
     		doc.put("counter", 1L);
     		this.collta.insert(doc);
     		//Long lObj1 = new Long(1);
     		log4j.info("end function for tweet id : " + tweet_id);
     	}

	log4j.info("end function for tweet id : " + tweet_id);
		
}
	//-------------------------------------------------------------------------------------------------------
	
	
	//The function returns  1 if the string is null,and 0 if the string str isn't null.
	public static int isNull(String str)
	{
		return ((str!=null)?0:1);
	}
	//--------------------------------------------------------------------------------------------------------
	//The function  assumes that String str is null,and then replace it with the empty string ""
	public static String replaceNull(String str)
	{
		return str=new String("");
	}
	//-------------------------------------------------------------------------------------------------------
	
	
	
	/*public static String right_place(Long NumTimeSlots,Long Timeslot,Long useid,DBCollection coll,String start,Long diff)
	 {
		 String s=useid.toString();
		 BasicDBObject query5 = new BasicDBObject();
		 query5.put("user_id",s);
		 DBCursor cursor6 = coll.find(query5);
		
		 while (cursor6.hasNext()&&cursor6.next()!=null)
		{
			//System.out.println(cursor6.curr());
		}
		 if( cursor6.curr()!=null )
		 {
			 String  ststart=cursor6.curr().get("palceflag").toString();
			 Long diffday=(diff/Timeslot)%NumTimeSlots;
			 diffday=(long) Math.floor(diffday);
			 
			 Long newday=new Long(ststart);
			 newday=(newday+diffday)%NumTimeSlots;
			 newday=(long) Math.floor(newday);

			 return newday.toString();
		 }
			
		 return "Error";
	 }
	*/
	
	//----This function getting search term and tweet id ----
	//----the function adding the tweet id and the time of search to the collection search_results--
		public void SearchResultId(String searchword,String tweet_id)
		{
			log4j.info("starting function SearchResultId with parameters: searchword = " + searchword + ", tweet_id" + tweet_id);
			try{
				DBObject searchobj = new BasicDBObject();
				searchobj.put("searchword", searchword);
				DBObject obj = this.collsr.findOne(searchobj);
				long min_time = System.currentTimeMillis() - this.frame_time;
				String[] tweets = obj.get("tweets").toString().split(",");
				String new_string = "";
				for (int i=1;i<tweets.length;i+=2)
				{
					if (Long.parseLong(tweets[i]) >= min_time)
					{
						if (new_string == "")
						{
							new_string += tweets[i-1] + "," + tweets[i];
						}
						else
						{
							new_string += "," + tweets[i-1] + "," + tweets[i];
						}
					}
				}
				obj.put("tweets", new_string + "," + tweet_id);
				this.collsr.save(obj);
			}
			catch (NullPointerException e){
				DBObject searchobj = new BasicDBObject();
				searchobj.put("searchword", searchword);
				searchobj.put("tweets", tweet_id);
				this.collsr.save(searchobj);
			}
			log4j.info("ending function SearchResultId");
			
		}
	 
	 
		
		//----This function searching the data Base and finds the tweet with the same tweet_id----
		//---------the function was given,and returns the text of the tweet as a string-----------
		//----------------------------------------------------------------------------------------
		public String FromTweetIdToText(String TweetId)
		{
			//log4j.info("==================================================");
			//DBCollection coll=db.getCollection("all_tweets");
			BasicDBObject query5 = new BasicDBObject();
			query5.put("id",Long.parseLong(TweetId));
			DBObject cursor6 = this.collat.findOne(query5);//querying to find the document with the right tweet_id
				
			
				
				String str=cursor6.get("text").toString();
				//log4j.info("The text of the tweet was found:"+str);
				//System.out.println(str);
				return str;
			
			//log4j.warn("Error! The text of the tweet was'n found");
			//return null;//should'nt be
		}
		
		//--------------------------------------------------------------------------------------------------
		//----This function get the threshold value from the configuration file and the distance that-------
		//----the comparing function calculated,and if the distance is less or equal then the threshold,----
		//-------------------the function returns true,otherwise returns false------------------------------
		//--------------------------------------------------------------------------------------------------
		public static boolean thresholdfunction(int threshold,int funcDistance)
		{
			return(funcDistance<=threshold);
		}
		//-----------------------------------------------------------------------------------------------------------------
		
		//----This function sorting a given LinkedList of tweet_id to LinkedList of object's----
		//---------elements into ascending order,and returning the sorted LinkedList--------
		public LinkedList<String> sortTweetId(LinkedList<String> vc)
		{
			log4j.info("=========================================================");		  
		    Collections.sort(vc);	  
			return vc;
		}
		
		//--------------------------------------------------------------------------------------------------------------
		
		//---------------------------------------------------------------------------------------------------------
		//----This function get the LinkedList of Tweet_id's-vc,the threshold number,and the number of the function----
		//----we  want to compare the tweets with.for example if FunctionNum=1 ,we will use the Hamming Distance---
		//------------------and if FunctionNum=2,we will use levenshtein Distance ---------------------------------
		//---------------------------------------------------------------------------------------------------------
		public duplicateTweets[] compOneF(LinkedList<String> vc,int threshold,int FunctionNum)
		{
			String one,second;//will hold the strings of text from the tweets

			LinkedList<String> v=sortTweetId(vc);//sorting the LinkedList by tweet_id so that the earliest tweet will show first 

			duplicateTweets[] outarr=new duplicateTweets[v.size()];//creating array of duplicateTweets in the size of the LinkedList we got(v.size)
			 int[] myarr=new int[v.size()];
			
			 //here we creating a helper array which will be in the same size of the LinkedList we got-v,
			 //and initialized it to 1-symbolize, that this is the earliest tweet_id that maybe connected by similarity to 
			 //other tweets.
			 //on the other hand, 0-zero will symbolize,that the tweet_id has already been tested and found similar to previous tweet_id,
			 //so we don't need to take this tweet_id to consideration anymore
			 for(int i=0;i<(v.size());i++)
			 {
				 myarr[i]=1;
			 }		
				
			for(int firstTweetId=0;firstTweetId<(v.size());firstTweetId++)
			{
				if(myarr[firstTweetId]==1)//symbolize, that this is the earliest tweet_id that maybe connected 
				{						  //by similarity to other tweets,so we createing in the place firstTweetId,
										  //a new duplicateTweets
					outarr[firstTweetId]=new duplicateTweets(v.size());
					outarr[firstTweetId].setTweet_id(v.get(firstTweetId));
					//log4j.info("we createing in the place:"+firstTweetId+" ,a new duplicateTweets with Tweet_id:"+outarr[firstTweetId].getTweet_id());
					//System.out.println(outarr[firstTweetId].getTweet_id());							
				}
				else//myarr[firstTweetId]=0- symbolize,that the tweet_id has already been tested and found similar to previous tweet_id,
				{	//so we don't need to take this tweet_id to consideration anymore,hence the continue.
					continue;
				}
				
				for(int secondTweetId=firstTweetId+1;secondTweetId<v.size();secondTweetId++)
				{
					//Here we getting the text of the tweets,using the function FromTweetIdToText
					one=FromTweetIdToText(v.get(firstTweetId));
					second=FromTweetIdToText(v.get(secondTweetId));
					
					//making sure that the size of both tweet text is equal,and if not we cut them to an equal size					
					int sizeOne=one.length();
					int sizeSecond=second.length();
						
					if(one.length()<=second.length())
					{
						second=second.substring(0, sizeOne);
					}
					else//one.length()>second.length()
					{
						one=one.substring(0, sizeSecond);
					}
					//Here we use a Comparing class-TweetComparing
					TweetComparing TC=new TweetComparing(one,second,FunctionNum);
					int FuctionRes=TC.ValidationRules(FunctionNum);//using the comparing function to calculate the distance 
						
					if(thresholdfunction(threshold,FuctionRes)&&( myarr[secondTweetId]==1))//checking if the tweets_id are 
					{																	  //similar,and if so,initialize the
																						  //myarr array with 0,and updaing the
																						 //output array-outarr
						/*The updating*/
						outarr[firstTweetId].setIsunique(false);//Not unique anymore

						outarr[firstTweetId].str[secondTweetId]=new st();//creating the objects(=st) array str in(array) outarr--> st[] str;
						outarr[firstTweetId].str[secondTweetId].strTweet_id=new String(v.get(secondTweetId));
						outarr[firstTweetId].str[secondTweetId].grade=FuctionRes;
						//log4j.info("============================================================================");
						//log4j.info("The strTweet_id is:"+v.get(secondTweetId));
						//System.out.println("The strTweet_id is:"+v.get(secondTweetId));
						//System.out.println("The grade is:"+FuctionRes);
						//log4j.info("The grade is:"+FuctionRes);
													
						/*The initialization*/	
						myarr[secondTweetId]=0;
							
					}//end of the if
						
				}//end of the inner for loop
				
			}//end of for loop
			
			return outarr;
		}
		//---------------------------------------------------------------------------------------------------------------
		
		//-------------------------------------------------------------------------------------------
		//----This function is a wrapper function,that using the compOneF function,and getting-------
		//----the duplicateTweets array,and printing the array objects(The result) in the log file.--
		//-------------------------------------------------------------------------------------------
		public void TweetCompare(LinkedList<String> vc,int FunctionNum,int threshold)
		{
			
			log4j.info("==================================================");
			log4j.info("We useing the comparing function number-"+FunctionNum);
			vc = get_final_tweet_ids(vc);
			System.out.println("size of list: " + vc.size());
			duplicateTweets[] a=compOneF(vc,threshold,FunctionNum);	
			log4j.info("The result of this function is:");
			for(int i=0;i<=(a.length-1);i++)
			{
				if(a[i]!=null)
				{
					log4j.info("==================================================");
					log4j.info("Tweet ID: "+a[i].getTweet_id());
					log4j.info("Isunique is: "+a[i].isIsunique());
				}
				
				for(int j=0;j<=(a.length-1);j++)
				{
					if((a[i]!=null))
					{
						if(a[i].str[j]!=null)
						/*{
							log4j.info("In index "+j+" str is:null");
						}
						else*/
						{
							log4j.info("matching Tweet ID: "+a[i].str[j].strTweet_id);
							log4j.info("grade between id's: " + a[i].getTweet_id() + " and " + a[i].str[j].strTweet_id + " is:    " +a[i].str[j].grade);
						}
					}

				}

			}

		}
		//---------------------------------------------------------------------------------------------------------------
		
		
		public long GetRateTimeFrame(Long UserId,Long numofhours)
		{
			 this.log4j.info("======================================================");
			 long diff = numofhours*60*60*1000;
			 BasicDBObject docline = new BasicDBObject();
			 docline.put("user_id", UserId);//querying to find the right userId
			 DBObject doc = this.collrate.findOne(docline);
			 if (doc == null)
			 {
				 this.log4j.error("user id : " + UserId + " does not exist");
				 return -1L;
			 }
			 else
			 {
			 long result = 0;
			 long currstart = Long.parseLong(doc.get("current_slot_start_time_millis").toString());
			 if (System.currentTimeMillis() - diff > currstart)
			 {
				 this.log4j.info("result is 0");
				 return 0;
			 }
			 else
			 {
				 double backslots = diff/this.slot_time_millis;
				 if (backslots > this.num_of_slots)
				 {
					 this.log4j.info("you requested longer time than the time frame, the result will be only for the previous timeframe");
				 }
				 for (int i=0;i<backslots || i<this.num_of_slots;i++)
				 {
					 int slot = (int) ((this.current_slot_index - i + this.num_of_slots)%this.num_of_slots);
					 result += Long.parseLong(doc.get("slot" + slot).toString());
				 }
				 this.log4j.info("result is " + result);
				 return result;
			 }
			 }
			 			 
			 
		}
	

}
