package cs757.project.clustering;

import cs757.project.customkeys.Canopy;
import cs757.project.customkeys.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * Created by ed on 5/3/15.
 */
/**
 * @author aaronlee
 *
 *  5k users per map? 10M set has almost 70K users
 *  massaged data is almost 70MB, so about 5 MB per input
 *
 */
public class Step1UserToVector {

    public static class Step1UserToVectorMapper extends Mapper<Object, Text , Text, Text> {
        private static Map<Integer, String> userDictionary = new TreeMap<Integer, String>();
        static Text valOut = new Text();

        // Load the user dictionary into memory
        public void setup(Context  context) {
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                URI files[]= DistributedCache.getCacheFiles(context.getConfiguration());
                Path path = new Path(files[0].toString());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;
                line = reader.readLine();
                while (line != null) {
                    String[] _line = line.split("\t");
                    userDictionary.put(Integer.valueOf(_line[0]), _line[1]);
                    line = reader.readLine();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }//setup


        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //get user IDs
            String[] tokens = value.toString().split(",");
            for (String token: tokens) {
                int _token = Integer.valueOf(token);
                valOut.set(_token+"["+userDictionary.get(_token)+"]");
                context.write(key,valOut);
            }
        }


    }

    public static class Step1UserToVectorReducer  extends Reducer<Text, Text, Text, Text> {

        static Text valOut = new Text();
        static String content = "";
        public void reduce(Text key, Iterable<Text> userVectors, Context context) throws IOException, InterruptedException {
            for (Text userVector: userVectors) {
                content+= userVector.toString() +",";
            }
            //chop trailing comma
            content = content.substring(0, content.length()-1);
            valOut.set(content);
            context.write(key,valOut);
            content="";
        }

    }

}