//package cs757.project.clustering;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.TreeMap;
//
//import org.apache.commons.lang.StringUtils;
//
//public class ProjUtil {
//	
//	public static Map<String, Double> convertToMap(String line){
//		String tokens[] = line.split(",");
//		Map<String, Double> map = new TreeMap<String, Double>();//((int)(tokens.length/.94), 0.95f);
//		for ( String t : tokens ){
//			String[] keyValue = t.split(":");
//			map.put(keyValue[0], Double.valueOf(keyValue[1]));
//		}
//		return map;
//	}
//	
//	public static String mapToString(Map<String,Double> map){
//		List<String> items = new ArrayList<String>(map.size());
//		for ( Entry<String,Double> e : map.entrySet() )
//			items.add(e.getKey()+":"+e.getValue());
//		return StringUtils.join(items.toArray(), ",");
//	}
//
//}
