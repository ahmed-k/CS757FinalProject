package cs757.project.clustering.initialattempt;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

/**
 * author @aaronlee
 * author @ahmedalabdullah
 *
 * This class is a post-processing step for Step1WithReducer  , the Step1Reducer's output is a canopy-per-line, where each line consists of the following signature:
 * [centroid : members]
 * the centroid is expressed in its full vector form - userID[movie:rating, movie:rating...]
 * but for memory constraints and to speed things up, the members are just expressed as userIDs [user1, user2, user3...]
 *
 * This class takes the input from the Step1WithReducer class, then outputs
 */
public class Step1UserToVector {

    public static class Step1UserToVectorMapper extends Mapper<Text, Text, Text, Text> {
        private static Map<Integer, String> userDictionary = new TreeMap<Integer, String>();
        static Text keyOut = new Text();
        static String content = "";
        static Text valOut = new Text();

        // Load the user dictionary into memory
        public void setup(Context context) throws IOException {
            BufferedReader reader = null;
            try {
                System.out.println("GETTING CACHE FILES GETTING CACHE FILES...");
                FileSystem fs = FileSystem.get(context.getConfiguration());
                URI files[] = DistributedCache.getCacheFiles(context.getConfiguration());
                Path path = new Path(files[0].toString());
                reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;
                line = reader.readLine();
                while (line != null) {
                    String[] _line = line.split("\t");
                    userDictionary.put(Integer.valueOf(_line[0]), _line[1]);
                    line = reader.readLine();
                }
            } finally {
                reader.close();
            }
        }//setup


        //just a test for now to see what userDictionary actually contains
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //get user IDs
            System.out.println("VALUE IS" + value.toString());
            String[] tokens = value.toString().split(",");
            for (String token : tokens) {
                String tokenVal = userDictionary.get(Integer.valueOf(token));
                System.out.println("TOKEN IS :" + token);
                System.out.println("USERDIC VAL FOR TOKEN IS  IS :" + tokenVal);
                content += token + "[" + tokenVal + "],";
            }
            content = content.substring(0, content.length() - 1);
            valOut.set(content);
            context.write(key, valOut);
            content = "";

        }

    }


}


