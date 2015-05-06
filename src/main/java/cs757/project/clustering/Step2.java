package cs757.project.clustering;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * @author aaronlee
 *
 */
public class Step2 {
	
	public static class Step1Mapper extends Mapper<Object, Text , Text, Text> {
		
	    @Override
        protected void setup(Context context) throws IOException, InterruptedException{
        	super.cleanup(context);
        }
		
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        }
        
		@Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
        	super.cleanup(context);
        }
	    
    }
	
	public static class Step1Reducer  extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
	    }
	}
}
