import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.PriorityQueue;
import java.util.Comparator;

public class WordCount {
	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();					
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static class WordCountPair {
		private String word;
		private IntWritable count;
		public WordCountPair(String word, IntWritable count) {
			this.word = word;
			this.count = count;
		}
		
		public String getWord() { return this.word; }
		public IntWritable getCount() { return this.count; }
	}
	
	public static class WordCountComparator implements Comparator<WordCountPair> {	
		public int compare(WordCountPair p, WordCountPair q) {
			int count1 = p.getCount().get();
			int count2 = q.getCount().get();
			if (count1 == count2) return 0;
			return count1 > count2 ? 1 : -1;
		}		
	}

	public static class SortMapper extends Mapper<Text, Text, Text, IntWritable>{
		private PriorityQueue<WordCountPair> pq = new PriorityQueue<WordCountPair>(10, new WordCountComparator());
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			pq.add(new WordCountPair(key.toString(), new IntWritable(Integer.parseInt(value.toString()))));		
			if (pq.size() > 10) {
				pq.remove();
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException{
			while(pq.size() > 0){
				WordCountPair p = pq.remove();
				context.write(new Text(p.getWord()), p.getCount());
			}
		}
	}
		
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path intermediatePath = new Path("intermediate_output");
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);		
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, intermediatePath);
		job.waitForCompletion(true);
		
		Job job2 = Job.getInstance(conf, "top 10");
		job2.setJarByClass(WordCount.class);
		job2.setMapperClass(SortMapper.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job2, intermediatePath);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}	
}
