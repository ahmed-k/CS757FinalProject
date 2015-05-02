package cs757.project.driver;

import org.apache.hadoop.mapreduce.Job;

/**
 * Created by alabdullahwi on 5/2/2015.
 */
public class ProjectDriver {

    //args should be [input] [output] [kind of job]
    public static void main(String[] args) throws Exception {
        Job job = JobFactory.createJob(args);
        System.exit(job.waitForCompletion(true)? 0: 1);
    }
}
