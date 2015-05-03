package cs757.project.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cs757.project.Step1;
import cs757.project.preprocessor.Munger;

/**
 * Created by alabdullahwi on 5/2/2015.
 */
public class JobFactory {


    public static Job createJob(String[] args) throws IOException {

        if ("preprocess".equals(args[2])) {
           return createPreprocessingJob(args);
        } else if ( "step1".equals(args[2]) )
        	return createStep1Job(args);
        
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
        clearOutputDir(conf, outputDir);

        return retv;
    }
    
	private static Job createStep1Job(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.setInt("mapreduce.input.lineinputformat.linespermap", 1500);
        
        Job job = new Job(conf, "Step 1");
        job.setJarByClass(ProjectDriver.class);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapperClass(Step1.Step1Mapper.class);
        job.setReducerClass(Step1.Step1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);
        clearOutputDir(conf, outputDir);

        return job;
    	
    }
	
	private static void clearOutputDir(Configuration conf, Path outputDir) throws IOException {
    	FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
	}

}
