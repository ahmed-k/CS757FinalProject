package cs757.project.customkeys;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by alabdullahwi on 5/3/2015.
 */
public class User implements WritableComparable<User> {

    private String userId;
    private Map<String,Integer> ratings;

    public String toString() {
        String retv="ID:"+userId+"RATINGS:";
        for (Map.Entry<String,Integer> e : ratings.entrySet()) {
            retv+= "<"+ e.getKey()+ "," + e.getValue()+">";
        }
        return retv;
    }

    public User() {}
    public User(String id, Map<String,Integer> ratingsVector) {
        this.userId = id;
        this.ratings = ratingsVector;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
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
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(userId);
        int size = ratings.size();
        dataOutput.writeInt(size);
        for (Map.Entry<String,Integer> rating : ratings.entrySet()) {
            dataOutput.writeUTF(rating.getKey());
            dataOutput.writeInt(rating.getValue());
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userId = dataInput.readUTF();
        int size = dataInput.readInt();
        ratings = new HashMap<String,Integer>((int) (size/.745));
        for (int i = 0 ; i < size ; i++ ) {
            String key = dataInput.readUTF();
            Integer value = dataInput.readInt();
            ratings.put(key,value);
        }
    }
}
