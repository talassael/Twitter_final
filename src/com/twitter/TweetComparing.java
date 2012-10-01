package com.twitter;
/*The Use for this class Is by inserting a number of function that represents a specific comparing function
 * the ValidationRules function will create the specific class and activate the correct comparing algorithm
 * and then returns the calculated distance between the two strings s1, s2.*/

	public class TweetComparing
	{

		private String s1;
		private String s2;
		
		
		public TweetComparing(String s1,String s2,int num)//TweetComparing constructor
		{
			this.s1=s1;
			this.s2=s2;			
		}
		
		// This function according to int e=number of function,will activate the correct comparing algorithm
		// and returns the calculated distance between the two strings s1, s2.
		public int ValidationRules (int e)
		    {
				switch(e)
				{
					case 1:
					{
						HammingDistance h=new HammingDistance(s1,s2);
						return h.getDistance();
					}
					case 2:
					{
						return levenshteinDis.computeLevenshteinDistance(s1, s2);
					}				
				}
				return -1;//represent an error
		    }		
	}






