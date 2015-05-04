package cs757.project.driver;

import cs757.project.clustering.Step1;
import cs757.project.clustering.Step1UserToVector;
import cs757.project.clustering.Step1WithReducer;
import cs757.project.customkeys.Canopy;
import cs757.project.preprocessor.Munger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * Created by alabdullahwi on 5/2/2015.
 */
public class JobFactory {


    //args[0] is input, args[1] is output dir, args[2] is jobType
    public static Job createJob(String[] args) throws Exception {
        String jobType = args[2];
        if ("preprocess".equals(jobType)) {
            return createPreprocessingJob(args);
        } else if ("step1".equals(jobType)) {
            return createStep1Job(args);
        } else if ("step1reduce".equals(jobType)) {
            return createStep1JobWithReducer(args);
        } else if ("step1vector".equals(jobType)) {
            return createStep1PostProcessJob(args);
        }
        return null;
    }

    //placeholder
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

    private static Job createStep1JobWithReducer(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.setInt("mapreduce.input.lineinputformat.linespermap", 1500);

        Job job = new Job(conf, "Step 1 with Reducer");
        job.setJarByClass(ProjectDriver.class);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapperClass(Step1WithReducer.Step1Mapper.class);
        job.setReducerClass(Step1WithReducer.Step1Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Canopy.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);
        clearOutputDir(conf, outputDir);

        return job;

    }

    private static Job createStep1PostProcessJob(String [] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Step 1 User To User Vector Postprocess Job");
        DistributedCache.addCacheFile(new URI("original_inputs/massaged.dat"), conf);
        job.setJarByClass(ProjectDriver.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(Step1UserToVector.Step1UserToVectorMapper.class);
        job.setReducerClass(Step1UserToVector.Step1UserToVectorReducer.class);
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
