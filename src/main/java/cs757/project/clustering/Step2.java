package cs757.project.clustering;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import cs757.project.customkeys.User;


/**
 * @author aaronlee
 *
 */
public class Step2 {
	
	public static class Step2Mapper extends Mapper<Text, Text , Text, Text> {
		
		static List<Centroid> centroids;
		static Text keyOut, valueOut;
		
	    @Override
        protected void setup(Context context) throws IOException, InterruptedException{
	    	centroids = new ArrayList<Centroid>();
	    	keyOut = new Text();
	    	valueOut = new Text();
	    	
	    	BufferedReader reader = null;
            try {
                System.out.println("GETTING CACHE FILES GETTING CACHE FILES...");
                FileSystem fs = FileSystem.get(context.getConfiguration());
                URI files[] = DistributedCache.getCacheFiles(context.getConfiguration());
                Path path = new Path(files[0].toString());
                reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line = reader.readLine();
                while (line != null) {
                    String[] centroidCounts = line.split("::");
                    centroids.add(new Centroid(centroidCounts[0], centroidCounts[1]));
                    line = reader.readLine();
                }
            } finally {
                reader.close();
            }
            if ( centroids.isEmpty() )
            	throw new RuntimeException("no centroids found within distributed cache!");
            
            System.out.println("total centroids="+centroids.size());
        }
		
	    @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	Map<String,Integer> userRatings = User.convertToMap(value.toString());
        	
        	List<Centroid> candidates = new ArrayList<Centroid>();
        	for ( Centroid c : centroids )
        		if ( Distance.jaccardCentroid(c, userRatings) > minSimilarity )
        			candidates.add(c);
        	
        	System.out.println("candidates size="+candidates.size());
        	Double min = 99999.9;
        	Centroid cluster;
        	for ( Centroid c : candidates ){
        		Double d = Distance.euclidean(c.centroid, userRatings, c.weights);
        		if ( d < min ){
        			min = d;
        			cluster = c;
        		}
        	}
        	
        	if ( cluster != null ){
        		keyOut.set(Centroid.mapToString(cluster.centroid));
        		context.write(keyOut, key);
        	} else {
        		keyOut.set("unclustered");
        		context.write(keyOut, key);
        	}
        	
        }
	    
    }
	
	public static class Step2Reducer  extends Reducer<Text, Text, Text, Text> {
		
		static Text valueOut = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String> outputVal = new ArrayList<String>();
			for ( Text t : values )
				outputVal.add(t.toString());
			valueOut.set(StringUtils.join(outputVal.toArray(), ","));
			context.write(key, valueOut);
	    }
	}
}
