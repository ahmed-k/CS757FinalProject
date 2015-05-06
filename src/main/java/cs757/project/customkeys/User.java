package cs757.project.customkeys;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.WritableComparable;

/**
 * Created by alabdullahwi on 5/3/2015.
 */
public class User implements WritableComparable<User> {

    private int userId;
    private Map<String,Integer> ratings;

    public String toString() {
        String retv=userId+"[";
        for (Map.Entry<String,Integer> e : ratings.entrySet()) {
            retv+= e.getKey()+ ":" + e.getValue()+",";
        }
        retv = retv.substring(0, retv.length()-1);
        retv += "]";
        return retv;
    }

    public boolean equals(Object o) {
        if (o == null || o instanceof User == false) {
            return false;
        }
        User other = (User) o ;
        return other.getUserId() == this.userId;

    }

    public int hashCode() {
        return ratings.hashCode() % userId * 37 ;

    }

    public User() {}
    public User(int id, Map<String,Integer> ratingsVector) {
        this.userId = id;
        this.ratings = ratingsVector;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public Map<String, Integer> getRatings() {
        return ratings;
    }

    public void setRatings(Map<String, Integer> ratings) {
        this.ratings = ratings;
    }

    @Override
    public int compareTo(User o) {
        if (userId > o.getUserId()) {
            return 1;
        }
        else if (userId == o.getUserId()) {
            return 0;
        }
        else {
            return -1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(userId);
        int size = 0;
        if (ratings != null) {
            size = ratings.size();
        }
        dataOutput.writeInt(size);
        if ( ratings != null) {
            for (Map.Entry<String, Integer> rating : ratings.entrySet()) {
                dataOutput.writeUTF(rating.getKey());
                dataOutput.writeInt(rating.getValue());
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userId = dataInput.readInt();
        int size = dataInput.readInt();
        if (size != 0) {
            ratings = new HashMap<String,Integer>((int) (size/.745));
            for (int i = 0 ; i < size ; i++ ) {
                String key = dataInput.readUTF();
                Integer value = dataInput.readInt();
                ratings.put(key,value);
            }

        }
        else {
            ratings = new HashMap<String,Integer>();
        }
    }
    
	/**
	 *  this converts a line like in the example below into a map of string->int
	 * @param line example "121:6,245:8,244:4,97:2"
	 * @return
	 */
	public static Map<String, Integer> convertToMap(String line){
		String tokens[] = line.split(",");
		Map<String, Integer> map = new TreeMap<String, Integer>();//((int)(tokens.length/.94), 0.95f);
		for ( String t : tokens ){
			String[] keyValue = t.split(":");
			map.put(keyValue[0], Integer.valueOf(keyValue[1]));
		}
		return map;
	}
}
