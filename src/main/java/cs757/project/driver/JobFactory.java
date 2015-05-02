package cs757.project.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cs757.project.preprocessor.Munger;

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
//		conf.set("mapred.textoutputformat.separator", "::");
//        conf.set("mapreduce.input.fileinputformat.split.maxsize", "256000");
//        conf.set("mapreduce.job.reduces", "80");
        
        Job retv = new Job(conf, "Preprocessing Input for Distance Metric Calculation");
        retv.setJarByClass(ProjectDriver.class);
        retv.setInputFormatClass(TextInputFormat.class);
        retv.setMapperClass(Munger.MungerMapper.class);
        retv.setReducerClass(Munger.MungerReducer.class);
        retv.setOutputKeyClass(IntWritable.class);
        retv.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(retv, new Path(args[0]));
        
        Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(retv, outputDir);
        FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

        return retv;
    }



}
