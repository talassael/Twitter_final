package com.twitter;
/*This class represents a row with the followng filds:
 * the early tweet id that we are comparing with
 * the decision if this tweet is unique,or not
 * an array of st objects,that each object  represents a similar tweet.each st object will hold the following filds:String strTweet_id, and int grade
*/
public class duplicateTweets 
{
	private String tweet_id;
	private boolean Isunique;	
	
	public st[] str; 	
	
	//-------Setters & Getters-----------------------
	public void setIsunique(boolean isunique) {
		Isunique = isunique;
	}
	//------------------------------------------------
	public void setTweet_id(String tweet_id) {
		this.tweet_id = tweet_id;
	}
	//------------------------------------------------
	public boolean isIsunique() {
		return Isunique;
	}
	//------------------------------------------------
	public String getTweet_id() {
		return tweet_id;
	}
	//------------------------------------------------
	
	public duplicateTweets (int size)//duplicateTweets constructor
	{
		this.setTweet_id(null);
		this.setIsunique(true);
		str=new st[size];
	}
}
