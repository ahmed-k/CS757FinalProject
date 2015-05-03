package cs757.project.clustering;

import cs757.project.customkeys.Canopy;
import cs757.project.customkeys.User;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;


/**
 * @author aaronlee
 *
 *  5k users per map? 10M set has almost 70K users
 *  massaged data is almost 70MB, so about 5 MB per input 
 *
 */
public class Step1WithReducer {

    public static class Step1Mapper extends Mapper<Object, Text , Text, Canopy> {

        //this map will hold key: userID, the value is a map with pairs denoting movieID->rating
        static Map<String, Map<String,Integer>> userRatingsMap;
        static Random random = new Random();
        static Text keyOut = new Text("canopies");
        static Canopy _canopy = new Canopy();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            userRatingsMap = new HashMap<String,Map<String,Integer>>((int)(5000/.89), 0.9f);
            super.cleanup(context);
        }

        /**
         * the map method simply puts the file input into the userRatingsMap
         * @param key : userID
         * @param value : a line of movieID1:rating1,movieID2:rating2
         * @param context : Hadoop Mapper context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s");
            userRatingsMap.put(tokens[0], Distance.convertToMap(tokens[1]));
        }

        /**
         * once the map is built, emit canopies
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            emitCanopies(context);
            super.cleanup(context);
        }

        private void emitCanopies(Context context) throws IOException, InterruptedException {

            //5000 is the number of users per map node
            Set<Map<String,Integer>> canopized = new HashSet<Map<String,Integer>>((int)(5000/.745));
            int count = 0;
            int originalSize = userRatingsMap.size();
//			System.out.println("map size="+map.size());
            int limit = 50;

            while ( !userRatingsMap.isEmpty() && limit > 0 ){

                //create the list of users, then pick a random point out of set, u'
                List<String> users = new ArrayList<String>(userRatingsMap.keySet());
                int index = random.nextInt(userRatingsMap.size());
                String userPrime = users.get(index);

                //put u' into canopy and remove it from set
                Map<String, Integer> userPrimeVector = userRatingsMap.get(userPrime);
                User centroid = new User(userPrime, userPrimeVector);
                _canopy.setCentroid(centroid);
                userRatingsMap.remove(userPrime);

                List<String> veryCloseUsers = new ArrayList<String>();
                for ( String currentUser : userRatingsMap.keySet() ){
                    Map<String, Integer> currentUserVector = userRatingsMap.get(currentUser);
                    double similarity = Distance.jaccardBag(userPrimeVector, currentUserVector);
                    //T2
                    if ( similarity > 0.135 )
                        veryCloseUsers.add(currentUser);
                    //T1
                    if ( similarity > 0.120 ) {
                        User _user = new User(currentUser, currentUserVector);
                        _canopy.addUser(_user);
                    }
                }

//		        System.out.println("veryCloseUsers="+veryCloseUsers.size()+", canopy size="+canopy.size());
                //centroid = calcCentroid( canopy )
                //emit ( "centroid",centroid )
                if ( veryCloseUsers.size() > 10 || _canopy.getMembers().size()  > 50 ){
                    for ( String key : veryCloseUsers )
                        userRatingsMap.remove(key);
                    limit = 50;

                    //emit to reducer
                    context.write(keyOut, _canopy);
                    //canopized.addAll(canopy);
                    count++;
//			        System.out.println(count);
//			        System.out.println("total canopized="+canopized.size());
                } else {
                    limit--;
                }

//		        System.out.println("remaining set = "+map.size());
            }
            System.out.println("map size="+originalSize+", canopies found="+count+", canopized percentage="+canopized.size()/(double)originalSize);
            userRatingsMap.clear();
            _canopy.clear();
        }

    }

    public static class Step1Reducer  extends Reducer<Text, Canopy, Text, Text> {

        static Text keyOut;
        static Text valOut;

        public void reduce(Text key, Iterable<Canopy> canopies, Context context) throws IOException, InterruptedException {
            for (Canopy canopy : canopies) {
                keyOut.set(canopy.getCentroidId());
                valOut.set(canopy.printMembers());
                context.write(keyOut,valOut);
            }
        }
    }
}
