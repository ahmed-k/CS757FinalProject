package cs757.project.driver;

import cs757.project.preprocessor.Munger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by alabdullahwi on 5/2/2015.
 */
public class JobFactory {


    public static Job createJob(String[] args) throws IOException {

        if ("preprocess".equals(args[2])) {
           return createPreprocessingJob(args);
        }
        return null;
    }


    private static Job createPreprocessingJob(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Job retv = new Job(conf, "Preprocessing Input for Distance Metric Calculation");
        retv.setJarByClass(ProjectDriver.class);
        retv.setInputFormatClass(KeyValueTextInputFormat.class);
        retv.setMapperClass(Munger.MungerMapper.class);
        retv.setReducerClass(Munger.MungerReducer.class);
        retv.setOutputKeyClass(IntWritable.class);
        retv.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(retv, new Path(args[0]));
        FileOutputFormat.setOutputPath(retv, new Path(args[1]));

        return retv;
    }



}
