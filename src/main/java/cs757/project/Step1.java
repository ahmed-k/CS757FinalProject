package cs757.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * @author aaronlee
 *  
 *  5k users per map? 10M set has almost 70K users
 *  massaged data is almost 70MB, so about 5 MB per input 
 *
 */
public class Step1 {
	
	public static class Step1Mapper extends Mapper<Object, Text , Text, Text> {
		
		static Map<String, Map<String,Integer>> map;
		static Random random = new Random();
		
        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
        	map = new HashMap<String,Map<String,Integer>>((int)(5000/.89), 0.9f);
        	super.cleanup(context);
        }
		
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] tokens = value.toString().split("\\s");
        	map.put(tokens[0], Distance.convertToMap(tokens[1]));
        }
        
		@Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
        	emitCanopies(context);
        	super.cleanup(context);
        }
		
		private void emitCanopies(Context context) {
			
			Set<Map<String,Integer>> canopized = new HashSet<Map<String,Integer>>((int)(5000/.745));
			int count = 0;
			int originalSize = map.size();
//			System.out.println("map size="+map.size());
			int limit = 50;
			
			while ( !map.isEmpty() && limit > 0 ){
				
				//pick random point out of set, u'
		        List<String> users = new ArrayList<String>(map.keySet());		        
		        int index = random.nextInt(map.size());
		        String userPrime = users.get(index);
		        
		        //put u' into canopy and remove it from set
		        List<Map<String,Integer>> canopy = new ArrayList<Map<String,Integer>>();
		        canopy.add(map.get(userPrime));
		        Map<String, Integer> userPrimeVector = map.get(userPrime);
		        map.remove(userPrime);
		        
		        List<String> veryCloseUsers = new ArrayList<String>();
		        for ( String user : map.keySet() ){
		        	double similarity = Distance.jaccardBag(userPrimeVector, map.get(user));
		        	if ( similarity > 0.135 )
		        		veryCloseUsers.add(user);
		        	if ( similarity > 0.120 )
		        		canopy.add(map.get(user));
		        }
		        
//		        System.out.println("veryCloseUsers="+veryCloseUsers.size()+", canopy size="+canopy.size());
		        //centroid = calcCentroid( canopy )
		        //emit ( "centroid",centroid )
		        if ( veryCloseUsers.size() > 10 || canopy.size() > 50 ){
			        for ( String key : veryCloseUsers )
			        	map.remove(key);
			        limit = 50;
			        canopized.addAll(canopy);
			        count++;
//			        System.out.println(count);
//			        System.out.println("total canopized="+canopized.size());
		        } else {
		        	limit--;
		        }
		        
//		        System.out.println("remaining set = "+map.size());
		    }
			System.out.println("canopies found="+count+", canopized percentage="+canopized.size()/(double)originalSize);
			map.clear();
		}
        
    }
	
	public static class Step1Reducer  extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//			for ( Text t : values )
//				System.out.println(t.toString());
				
			
	    }
	}
}
