package com.twitter;

public class HammingDistance {

	//private String s1;
	//private String s2;

	/*public HammingDistance(String s1, String s2) {
		this.s1 = s1;
		this.s2 = s2;
	}*/

	public static int getDistance(String s1, String s2) {

		// check preconditions
		if (s1 == null || s2 == null || s1.length() != s2.length()) {
			throw new IllegalArgumentException();
		}

		// compute hamming distance
		int distance = 0;
		for (int i = 0; i < s1.length(); i++) {
			if (s1.charAt(i) != s2.charAt(i)) {
				distance++;
			}
		}
		return distance;

	}

	/*public double getDistanceSimilarity() {
		// TODO Not sure how to define this ?
		return 0;
	}
*/
}





