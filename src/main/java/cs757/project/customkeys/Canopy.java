package cs757.project.customkeys;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by alabdullahwi on 5/3/2015.
 */
public class Canopy implements WritableComparable<Canopy> {

    private User centroid;
    private List<User> members;


    public String getCentroidId() { return centroid.getUserId(); }
    public User getCentroid() { return centroid; }
    public void setCentroid(User user) { this.centroid = user; }
    public List<User> getMembers() { return members; }
    public void setMembers(List<User> members) { this.members = members; }

    public String printMembers() {
        String retv="";
        for (User member: members) {
            retv+= member.toString()+",";
        }
        //trailing comma chop
        retv = retv.substring(0, retv.length()-1);
        return retv;
    }

    public void addUser(User user) {
        if (members == null) {
            members=  new ArrayList<User>();
        }
        members.add(user);
    }

    public void clear() {
        centroid = null;
        members = null;
    }

    @Override
    public int compareTo(Canopy o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        centroid.write(dataOutput);
        dataOutput.writeInt(members.size());
        for (int i = 0 ; i< members.size(); i++) {
            members.get(i).write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        centroid = new User();
        int size = dataInput.readInt();
        centroid.readFields(dataInput);
        members = new ArrayList<User>((int)(size/.475));
        for (int i = 0 ; i< size; i++) {
            User _user = new User();
            _user.readFields(dataInput);
            members.add(_user);
        }
    }
}
