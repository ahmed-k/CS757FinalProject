package cs757.project.preprocessor;

/**
 * Created by alabdullahwi on 5/2/2015.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * this class takes the output produced by the non-MapReduce preprocessing program RemoveRareMovies
 *
 * input row example    :   movieID:userID:rating
 * output row example   :   userID: [movie:rating, movie:rating]
 *
 * it massages the data into the format expected by an arbitrary  Distance Measures to calculate Distance as the first (cheap) distance metric employed by Canopy Clustering
 *
 */
public class Munger {

    public static class MungerMapper extends Mapper<Object, Text , IntWritable, Text> {

        static IntWritable keyOut = new IntWritable();
        static Text valOut = new Text();

        public void map(Object _key, Text _vals, Context context) throws IOException, InterruptedException {
        	String[] vals = _vals.toString().split("::");
        	Integer userID = Integer.valueOf(vals[0]);
        	String movieID = vals[1];
        	String rating = vals[2];
            //normalize rating here
            keyOut.set(userID);
            valOut.set(movieID + ":" + new Double((Double.valueOf(rating) * 2)).intValue());
            context.write(keyOut, valOut);
        }
    }

        public static class MungerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

            static String content = "";
            static Text valOut = new Text();
            
            public void reduce(IntWritable userID, Iterable<Text> movieRatings, Context context) throws IOException, InterruptedException {
            	List<String> ratings = new ArrayList<String>();
                for (Text mr : movieRatings)
                	ratings.add(mr.toString());
                valOut.set(StringUtils.join(ratings, ","));
                context.write(userID, valOut);
            }
        }

}
