package cs757.project;

import java.util.HashMap;
import java.util.Map;

public class Distance {
	
	public static double jaccardBag(String[] v1, String[] v2){
		
		int intersection = 0;
		int union = 0;
		
		for ( int i = 0; i < v1.length; i++ ){
			String r1 = v1[i];
			String r2 = v2[i];
			if ( !r1.isEmpty() && !r2.isEmpty() )
				intersection += Math.min(Integer.valueOf(r1), Integer.valueOf(r2));
			if ( !r1.isEmpty() )
				union += Integer.valueOf(r1);
			if ( !r2.isEmpty() )
				union += Integer.valueOf(r2);
		}
		return intersection/(double)union;
	}
	
	
//	public static Map<Integer, String> convertToMap(String[] vector){
//		Map<Integer, String> map = new HashMap<Integer, String>((int)(vector.length/.74));
//		for ( int i = 0; i < vector.length; i++ )
//		    if ( !vector[i].isEmpty() )   
//		         map.put( i+1 , vector[i] );  
//		return map;
//	}
	

}