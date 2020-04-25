// Version 3 - removes punctuation around words
// set up the standard libraries
import java.io.IOException;
// replace StringTokenizer with regex Pattern
import java.util.regex.Pattern;
// set up the Hadoop libraries
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
  // Mapper class
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
    // one is simply a variable to store a word count of 1
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text(); // use Text for character data
    // context will be used to emit the output values
    private boolean caseSensitive = false;
    // set up a regular expression for word boundaries
    // these include spaces, tabs and punctuation
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    // added to make Word Count case insensitive
    // Hadoop will call this method automatically when a job is submitted
    protected void setup(Mapper.Context context)
        throws IOException, InterruptedException {
	Configuration config = context.getConfiguration();
	this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
	} // setup method

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString();
      // check if job is case sensitive or not
      if (!caseSensitive) 
	line= line.toLowerCase();

     Text currentWord = new Text();
     // split the line into individual words based on word boundaries
     // if the word is empty then move on
     for (String word : WORD_BOUNDARY.split(line)) {
     	if (word.isEmpty()) {
            continue;
            }

        if (word.contains("[") ||word.contains("'")||word.contains("-")||word.contains("--") || word.contains("#") ||word.contains("&")
        	|| word.contains("!") ||word.contains(";"))
        	{
            continue;
            }
        currentWord = new Text(word);
        context.write(currentWord,one); // this is what is output by mapping stage
        } // for loop
    } // map method
  } // TokenizeMapper class

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    // this will read in the output generated by the map function

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      // use IntWriteable for numeric values
      // this will add up all the words with the same name
      for (IntWritable val : values) {
        sum += val.get();
      } // for loop
      result.set(sum);
      // this outputs the final result
      context.write(key, result);
    } // reduce method
  } // IntSumReducer class

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    // give the job a name
    Job job = Job.getInstance(conf, "word count");
    // tell Hadoop which jar file to use
    job.setJarByClass(WordCount.class);
    // tell Hadoop which Mapper class to use
    job.setMapperClass(TokenizerMapper.class);
    // tell Hadoop which Combination class to use
    // used to summarise map output with the same key
    // Combiner is also known as a semi-reducer
    job.setCombinerClass(IntSumReducer.class);
    // tell Hadoop which Reducer class to use
    job.setReducerClass(IntSumReducer.class);
    // set the type of the key and value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    // defines how many arguments are expected
    // can have more than one input directory
    // args[0] will be the input directory and args[1] the output
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    // will stop when the job completes
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  } // main
} // WordCount class
