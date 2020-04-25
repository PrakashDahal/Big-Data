/*
   ***************************************************
   Java file to total up the figures in the csv files
   Amended to work with Floats or Numbers
   MG March 2019
   ***************************************************
*/

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
 public class NoQuals {
 
 public static class PDMapper extends Mapper <Object, Text, Text, Text>  {
	public void map(Object key, Text value, Context context) 
		throws IOException, InterruptedException {

	String record = value.toString();
	String[] parts = record.split(",");
	// 0: Key (county) 1: Year 2: figure
	// need to deal with null values - defaults to 0 
	if (parts.length == 3 )
		context.write(new Text(parts[0]), new Text(parts[2]));
	else
		context.write(new Text(parts[0]), new Text("0"));
		} // map
	} // PopMapper
 
	public static class PDReducer extends Reducer <Text, Text, Text, Text> {
		public static boolean isInteger(String s) {
    		  try { 
        		Integer.parseInt(s); 
 		   	} catch(NumberFormatException e) { 
        		return false; 
    			} catch(NullPointerException e) {
        		return false;
    		  	}
    			// only get here if a number
    		  return true;
		} // isInteger

		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException  {
		String popName = "";
		double total = 0;
		int count = 0;
		boolean isInt = false;
		// a basic test to test for number/float
		// assumes is a float if not a number
		for (Text t : values) { 
			String parts[] = t.toString().split("\t");
			count++;
			isInt = isInteger(parts[0]);
			if (isInt)
				total += Integer.parseInt(parts[0]);
			else
				total += Float.parseFloat(parts[0]);
			} // for loop
		// format count to 2 decimal places
		String str = String.format("%d,%.2f", count, total);
		context.write(new Text(key), new Text(str));
		} //reduce
	} // csvReducer
 
 public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	//set output delimiter to comma
	conf.set("mapreduce.output.textoutputformat.separator", ","); 
 
	Job job = Job.getInstance(conf, "No Quals Count");
	job.setJarByClass(NoQuals.class);
	job.setMapperClass(PDMapper.class);
	job.setReducerClass(PDReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	Path outputPath = new Path(args[1]);
	FileOutputFormat.setOutputPath(job, outputPath);

	// Delete the output directory - true means if path is a directory it does recursive delete
	outputPath.getFileSystem(conf).delete(outputPath, true);
	System.exit(job.waitForCompletion(true) ? 0 : 1);
 }	// main
} // Population class
