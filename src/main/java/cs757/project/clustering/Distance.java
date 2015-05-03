package cs757.project.clustering;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Distance {
	
	/**
	 * @param line1 example "121:6,245:8,244:4,97:2"
	 * @param line2 same structure as line1
	 * @return jaccard bag similarity
	 */
	public static double jaccardBag(String line1, String line2){
		return jaccardBag(convertToMap(line1), convertToMap(line2));
	}
	
	public static double jaccardBag(Map<String,Integer> map1, Map<String,Integer> map2){
		
		int intersection = 0;
		int union = 0;
		
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
	
	private static Double weight;
	private static double diffSqr;
	
	/**
	 * Using weights will produce a different scale
	 * ideally weights are used when comparing a centroid map1 to point map2
	 * 
	 * @param map1
	 * @param map2
	 * @param weights if weights are used, it must have at least the same keys as map1.
	 * @return
	 */
	public static double euclidean(Map<String,Integer> map1, Map<String,Integer> map2, Map<String,Double> weights){
		double sum = 0;
		for (String key : map1.keySet() ){
			Integer v1 = map1.get(key),
					v2 = map2.get(key);
			if ( v1 != null && v2 != null ){
				diffSqr = Math.pow(v1 - v2, 2);
				if ( weights != null ){
					weight = weights.get(key);
					if ( weight != null )
						diffSqr = diffSqr*weight;
				}
				sum += diffSqr;
			}
		}
		return sum == 0 ? 999999 : Math.sqrt(sum);
	}
	
	public static double cosine(Map<String,Integer> map1, Map<String,Integer> map2){
		
		Integer sum = 0;
		int count = 0;
		for ( String key : map1.keySet() ){
			sum += map1.get(key);
			count++;
		}
		
		double xMean = sum/(double)count;
		sum = 0;
		count = 0;
		for ( String key : map2.keySet() ){
			sum += map2.get(key);
			count++;
		}
		double yMean = sum/(double)count;
		
		Set<String> keys = new HashSet<String>(map1.keySet());
		keys.addAll(map2.keySet());
		double sumXY = 0.0,
				xSqrSum = 0.0,
				ySqrSum = 0.0;
		
		for ( String key : keys ){
			Integer x = map1.get(key),
					y = map2.get(key);
			if ( x != null && y != null )
				sumXY += (x-xMean)*(y-yMean);
			if ( x != null )
				xSqrSum = Math.pow(x-xMean, 2);
			if ( y != null )
				ySqrSum = Math.pow(y-yMean, 2);
		}
		return sumXY/(Math.sqrt(xSqrSum)*Math.sqrt(ySqrSum));
	}
		
	/**
	 *  this converts a line like in the example below into a map of string->int
	 * @param line example "121:6,245:8,244:4,97:2"
	 * @return
	 */
	public static Map<String, Integer> convertToMap(String line){
		String tokens[] = line.split(",");
		Map<String, Integer> map = new TreeMap<String, Integer>();//((int)(tokens.length/.94), 0.95f);
		for ( String t : tokens ){
			String[] keyValue = t.split(":");
			map.put(keyValue[0], Integer.valueOf(keyValue[1]));
		}
		return map;
	}
	

}