package cs757.project.preprocessor;

/**
 * Created by alabdullahwi on 5/2/2015.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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
        	String _rating = vals[2];
            String movieID = vals[1];
            //normalize rating here
            int rating = new Double((Double.valueOf(_rating) * 2)).intValue();
            keyOut.set(userID);
            valOut.set(movieID + ":" + rating);
            context.write(keyOut, valOut);
        }
    }


        public static class MungerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

            static String content = "";
            static Text valOut = new Text();
            public void reduce(IntWritable userID, Iterable<Text> movieRatings, Context context) throws IOException, InterruptedException {
                for (Text movieRating : movieRatings) {
                    content += movieRating.toString() + ",";
                }
                //remove last comma
                content = content.substring(0, content.length());
                valOut.set(content);
                content = "";
                context.write(userID, valOut);
                }
            }


        }

