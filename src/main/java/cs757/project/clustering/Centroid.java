package cs757.project.clustering;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;

public class Centroid {
	
	protected final Map<String,Double> centroid;
	protected final Map<String,Double> weights;
	
	public Centroid(String centroid, String weights){
		this.centroid = convertToMap(centroid);
		this.weights = convertToMap(weights);
	}
	
	@Override
	public int hashCode(){
		return this.weights.hashCode();
	}
	
	@Override
	public boolean equals(Object o){
		Centroid that = (Centroid)o;
		return mapToString(this.weights).equals(mapToString(that.weights));
	}
	
	@Override
	public String toString(){
		return mapToString(centroid)+"\n"+mapToString(weights);
	}

	public void combine(Centroid c) {
		for ( String movie : c.centroid.keySet() ){
			Double prevRating = this.centroid.get(movie);
			if ( prevRating == null ){
				this.centroid.put(movie, c.centroid.get(movie));
				this.weights.put(movie, c.weights.get(movie));
			} else {
				Double prevCount1 = this.weights.get(movie);
				Double prevCount2 = c.weights.get(movie);
				Double totalCount = prevCount1 + prevCount2; 
				Double newRating = (prevCount1/totalCount)*prevRating + (prevCount2/totalCount)*c.centroid.get(movie);
				this.centroid.put(movie, newRating);
				this.weights.put(movie, totalCount);
			}
		}
	}
	
	public static Map<String, Double> convertToMap(String line){
		String tokens[] = line.split(",");
		Map<String, Double> map = new TreeMap<String, Double>();//((int)(tokens.length/.94), 0.95f);
		for ( String t : tokens ){
			String[] keyValue = t.split(":");
			map.put(keyValue[0], Double.valueOf(keyValue[1]));
		}
		return map;
	}
	
	public static String mapToString(Map<String,Double> map){
		List<String> items = new ArrayList<String>(map.size());
		for ( Entry<String,Double> e : map.entrySet() )
			items.add(e.getKey()+":"+e.getValue());
		return StringUtils.join(items.toArray(), ",");
	}
}