package cs757.project.customkeys;

import org.apache.commons.lang.StringUtils;
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
    private List<String> members;

    public Canopy() {
        centroid = new User();
        members = new ArrayList<String>();
    }

    public User getCentroid() { return centroid; }
    public void setCentroid(User user) { this.centroid = user; }
    public List<String> getMembers() { return members; }
    public void setMembers(List<String> members) { this.members = members; }

    public String printMembers() {
//        String retv="";
//        for (String member: members) {
//            retv+= member.toString()+",";
//        }
//        //trailing comma chop
//        retv = retv.substring(0, retv.length()-1);
//        return retv;
        
        //this may just work as well
        return StringUtils.join(members.toArray(), ",");
    }

    public void addUser(String user) {
        if (members == null) {
            members=  new ArrayList<String>();
        }
        members.add(user);
    }

    public void clear() {
        centroid = null;
        members.clear();
    }

    @Override
    public int compareTo(Canopy o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        centroid.write(dataOutput);
        int size = 0 ;
        if (members != null) {
            size = members.size();
        }
        dataOutput.writeInt(size);
        for (int i = 0 ; i< size ; i++) {
            dataOutput.writeUTF(members.get(i));
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        centroid = new User();
        centroid.readFields(dataInput);
        int size = dataInput.readInt();
        if (size != 0) {
            members = new ArrayList<String>((int) (size / .475));
            for (int i = 0; i < size; i++) {
                String _user = dataInput.readUTF();
                members.add(_user);
            }
        }
        else {
            members = new ArrayList<String>();
        }
    }
}
