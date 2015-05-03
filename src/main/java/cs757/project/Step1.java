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
	
	public static class Step1Mapper extends Mapper<Text, Text , Text, Text> {
		
		static Map<String, String> map;
		static Random random = new Random();
		
        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
        	map = new HashMap<String,String>((int)(5000/.745));
        	super.cleanup(context);
        }
		
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	map.put(key.toString(), value.toString());
        	if ( map.size() == 5000 )
        		emitCanopies(context);
        }
        
		@Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
        	emitCanopies(context);
        	super.cleanup(context);
        }
		
		private void emitCanopies(Context context) {
			
			while ( !map.isEmpty() ){
		        
				//pick random point out of set, u'
		        List<String> users = new ArrayList<String>(map.keySet());
		        
		        int index = random.nextInt(map.size());
		        String userPrime = users.get(index);
//		        System.out.println(userPrime);
		        
		        //put u' into canopy and 
		        List<String> canopy = new ArrayList<String>();
		        canopy.add(map.get(userPrime));
		        Map<String, Integer> userPrimeVector = Distance.convertToMap(map.get(userPrime));
		        map.remove(userPrime);
		        
		        /*
		        for each u in set
		           if u veryClose to u'
		               veryCloseCollection.put(u)
		           if u closeEnough to u'
		               canopySet.put(uVector)
		        */
		        List<String> veryCloseUsers = new ArrayList<String>();
		        for ( String user : map.keySet() ){
		        	double d = Distance.jaccardBag(userPrimeVector, Distance.convertToMap(map.get(user)));
		        	if ( d <= 0.0125 )
		        		veryCloseUsers.add(user);
		        	else if ( d <= 0.025 )
		        		canopy.add(map.get(user));
		        }
		        
		        System.out.println("veryCloseUsers ="+veryCloseUsers.size()+", canopy size = "+canopy.size());
		        //centroid = calcCentroid( canopySet )
		        //emit ( "centroid",centroid )
		        //set.remove( veryCloseSet )
		        for ( String key : veryCloseUsers )
		        	map.remove(key);
		        
		        System.out.println("remaining set = "+map.size());
		    }
			
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
