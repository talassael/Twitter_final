package com.twitter;


import java.util.List;
import java.util.Properties;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

/*-----Not using for now-----------------*/
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
/*-----Not using for now-----------------*/
import com.mongodb.DBCursor;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
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




//-----------------------------------------------------------------
@SuppressWarnings("unused")
public class DataMine 
{

	
		
		public static void main(String[] args) throws InterruptedException,TwitterException,UnknownHostException, MongoException 

		{
			final String frame, slots;
			final double  num_of_slots;
			final double max_time_frame_hours;
			final String[] rateparam , comparam;
			final int threshhold, mode;
			//	how to retrive data from configuration file (.properties file) 
			Properties prop = new Properties();
			try {
				prop.load(new FileInputStream("twitterconfig.properties")); // load the file
			}
			catch (Exception e){
				e.printStackTrace();
			}
			slots = prop.getProperty("slots");
			frame = prop.getProperty("frame");
			rateparam = prop.getProperty("rateparam").split(",");
			comparam = prop.getProperty("comparam").split(",");
			mode = Integer.parseInt(prop.getProperty("mode"));
			threshhold = Integer.parseInt(prop.getProperty("threshhold").toString());
			int depth = Integer.parseInt(prop.getProperty("depth").toString());
			max_time_frame_hours = Long.parseLong(frame);
			num_of_slots = Double.parseDouble(slots);
			long frame_time = (long)(max_time_frame_hours * 60*60*1000);
			
			double slot_time_millis = (max_time_frame_hours/num_of_slots)*60*60*1000;
				
				
			Mongo mongo=new Mongo("localhost",27017);
			final DB db = mongo.getDB("twitter");//Creating the name of the new DB
			
			final Processor proc = new Processor(mongo , db , slot_time_millis , frame_time , threshhold , depth , (long)num_of_slots);
			
			if (mode == 10)
			{
				Thread t1=new Thread(new Runnable(){
					public void run(){
						try {
							proc.update_all_tweets(num_of_slots , max_time_frame_hours);
						} catch (ParseException e) {
							e.printStackTrace();
						}
					}
				});
				
				Thread t2=new Thread(new Runnable(){
					public void run(){
						proc.update_tree();
					}
				});
				t1.setPriority(Thread.MAX_PRIORITY);
				
				
				t2.start();
				t1.start();
			}
			else if (mode == 20)
			{
				proc.GetRateTimeFrame(Long.parseLong(rateparam[0]), Long.parseLong(rateparam[1]));
			}
			
			else
			{
				LinkedList <String> searchlist = new LinkedList <String>(Arrays.asList(comparam));
				proc.TweetCompare(searchlist, mode, threshhold);
			}
			
			//String[] a = {"#global"};
			//neo4j.addNode("@nathanhowell", a , proc.log4j);
			//proc.TweetCompare(searchlist, 2, proc.threshold);

		}//close main

	}

	
	