package cs757.project.clustering;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import cs757.project.customkeys.User;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;


public class Step1 {
	
	public static class Step1Mapper extends Mapper<Object, Text , Text, Text> {
		
		static Map<String, Map<String,Integer>> map;
		static Random random = new Random();
		static Text keyOut = new Text("centroids");
		static Text valueOut = new Text();
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
        	map = new HashMap<String,Map<String,Integer>>((int)(1500/.89), 0.9f);
        	super.cleanup(context);
        }
		
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] tokens = value.toString().split("\\s");
        	map.put(tokens[0], User.convertToMap(tokens[1]));
        }
        
		@Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
        	emitCanopies(context);
        	super.cleanup(context);
        }
		
		private void emitCanopies(Context context) throws IOException, InterruptedException {
			
			//Set<Map<String,Integer>> canopized = new HashSet<Map<String,Integer>>((int)(map.size()/.89), 0.9f);
			int numberOfCanopies = 0;
			int originalSize = map.size();
			int limit = 50;
			int vcMin = (int)(originalSize*.02);
	        int canopyMin = (int)(originalSize*.05);
			
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
		        if ( veryCloseUsers.size() > vcMin || canopy.size() > canopyMin ){
			        for ( String key : veryCloseUsers )
			        	map.remove(key);
			        limit = 50;
			        
			        Map<String,Double>[] centroidCounts = calcCentroid(canopy);			        
			        Map<String,Double> centroid = centroidCounts[0];
			        Map<String,Double> movieCounts = centroidCounts[1];
			        
//			        valueOut.set(Centroid.mapToString(centroid)+"::"+Centroid.mapToString(movieCounts));
			        valueOut.set(Centroid.mapToString(centroid)+"::+");
//			        System.out.println("users remaining="+map.size());
			        context.write(keyOut, valueOut);
			        numberOfCanopies++;
			        //canopized.addAll(canopy);
			        
		        } else {
		        	limit--;
		        }

		    }
			//System.out.println("map size=" + originalSize + ", canopies found=" + numberOfCanopies + ", canopized percentage=" + canopized.size() / (double) originalSize);
			System.out.println("map size=" + originalSize + ", canopies found=" + numberOfCanopies);
			//TODO emit number of canopies
			valueOut.set(String.valueOf(numberOfCanopies));
			context.write(keyOut, valueOut);
			
			map.clear();
		}
		
		@SuppressWarnings("unchecked")
		public static Map<String, Double>[] calcCentroid(List<Map<String,Integer>> canopy){
			
			Map<String,Double> centroid = new HashMap<String,Double>();
			Map<String,Double> counts = new HashMap<String,Double>();
			///int numberOfMovies = 0;
			
			for ( Map<String,Integer> userVector : canopy ){
				for ( Entry<String,Integer> movieRating : userVector.entrySet() ){
					Double prev = centroid.get(movieRating.getKey());
					centroid.put(movieRating.getKey(), prev == null ? movieRating.getValue() : prev+movieRating.getValue());
					prev = counts.get(movieRating.getKey());
					counts.put(movieRating.getKey(), prev == null ? 1 : prev+1 );
					//numberOfMovies++;
				}
			}
			
			List<String> removals = new ArrayList<String>();
			for ( Entry<String,Double> e : counts.entrySet() )
				if ( e.getValue() <= 2.0 )
					removals.add(e.getKey());
			for ( String key : removals ){
				counts.remove(key);
				centroid.remove(key);
			}
			
			
			for ( String movie : centroid.keySet() ){
				Double count = counts.get(movie);
				Double averageRating = centroid.get(movie)/count;
				centroid.put(movie, averageRating);
//				Double weight = count/numberOfMovies;
//				counts.put(movie, weight);
			}
			
			Map<String,Double> output[] = new Map[2];
			output[0] = centroid;
			output[1] = counts;
			return output;
		}
        
    }
	
	public static class Step1Reducer extends Reducer<Text, Text, Text, Text> {
		
		static Text keyOut = new Text("centroids");
		static Text valueOut = new Text();
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
        	super.setup(context);
        }
        
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			List<Integer> canopiesFound = new ArrayList<Integer>();
			List<Centroid> centroids = new ArrayList<Centroid>();
			
			for ( Text t : values ){
				String v = t.toString();
				if ( v.contains("::") ){	//is a centroid
					String centroidWeights[] = v.split("::");
					centroids.add(new Centroid(centroidWeights[0]));
				} else {
					canopiesFound.add(Integer.valueOf(v));
				}
			}
			
			Integer total = 0;
			for ( Integer i : canopiesFound )
				total += i;
			int avgCanopiesFound = total / canopiesFound.size();
			
			System.out.println("avg="+avgCanopiesFound);
			System.out.println("size="+centroids.size());
			
			Map<Centroid, Map<Centroid, Double>> alreadyCompared = new HashMap<Centroid, Map<Centroid, Double>>(); 
			
			while ( centroids.size() > avgCanopiesFound ){
				
				double max = -1.0;
				Centroid candidate1 = null, candidate2 = null;
				int indexForRemoval =  0;
				
				for ( int j = 0; j < centroids.size()-1; j++ ){
					Centroid c1 = centroids.get(j);
					for ( int k = j + 1; k < centroids.size(); k++ ){
						
						Centroid c2 = centroids.get(k);
						Double similarity = null;
						boolean calc = false;
						
						//begin optimization
						Map<Centroid,Double> compositeKey = alreadyCompared.get(c1);
						if ( compositeKey != null ){
							similarity = compositeKey.get(c2);
							if ( similarity == null )
								calc = true;
						} else 
							calc = true;
						
						if ( calc ){
//							System.out.println("calculating "+c1.id+", "+c2.id);
							similarity = Distance.cosine(c1.centroid, c2.centroid);
							if ( compositeKey == null )
								compositeKey = new HashMap<Centroid,Double>();
							compositeKey.put(c2, similarity);
							alreadyCompared.put(c1, compositeKey);
						} 
//						else
//							System.out.println("found "+c1.id+", "+c2.id);
						//end optimization
						
						if ( similarity > max ){
							indexForRemoval = j; //c1 will be removed
							max = similarity;
							candidate1 = c1;
							candidate2 = c2;
						}
					}
				}
				System.out.println("centroids remaining="+centroids.size());
				
				centroids.remove(indexForRemoval);
				candidate2.combine(candidate1);
//				System.out.println("removing "+candidate1.id);
				alreadyCompared.remove(candidate1);
				
			}
			
			keyOut.set("");
			for ( Centroid c : centroids ){
				valueOut.set(Centroid.mapToString(c.centroid));
//				valueOut.set(Centroid.mapToString(c.centroid)+"::"+Centroid.mapToString(c.weights));
				context.write(keyOut, valueOut);
			}
			
//			for ( int j = 0; j < centroids.size()-1; j++ ){
//				Centroid c1 = centroids.get(j);
//				for ( int k = j + 1; k < centroids.size(); k++ ){
//					Centroid c2 = centroids.get(k);
//					System.out.println(Distance.cosine(c1.centroid, c2.centroid));
//				}
//			}
			
	    }
		
		
		
	}
	
	
	
}
