package cs757.project;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Distance {
	
	/**
	 * @param line1 example "121:6,245:8,244:4,97:2"
	 * @param line2
	 * @return jaccard bag distance
	 */
	public static double jaccardBag(String line1, String line2){
		
		int intersection = 0;
		int union = 0;
		
		Map<String, Integer> map1 = convertToMap(line1),
							map2 = convertToMap(line2);
		
		Set<String> keys = new HashSet<String>(map1.keySet());
		keys.addAll(map2.keySet());
		for (String key : keys ){
			Integer v1 = map1.get(key),
					v2 = map2.get(key);
			if ( v1 != null && v2 != null )
				intersection += Math.min(v1, v2);
			if ( v1 != null )
				union += v1;
			if ( v2 != null )
				union += v2;
		}
		
		return intersection/(double)union;
	}
	
	/**
	 * @param line example "121:6,245:8,244:4,97:2"
	 * @return
	 */
	public static Map<String, Integer> convertToMap(String line){
		String tokens[] = line.split(",");
		Map<String, Integer> map = new HashMap<String, Integer>((int)(tokens.length/.74));
		for ( String t : tokens ){
			String[] keyValue = t.split(":");
			map.put(keyValue[0], Integer.valueOf(keyValue[1]));
		}
		return map;
	}
	

}