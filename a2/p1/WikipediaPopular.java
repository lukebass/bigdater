import java.io.IOException;

import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular extends Configured implements Tool {

	public static void getMaxViews(Iterable<LongWritable> values, LongWritable maxViews) {

		int currMax = 0;
		for (LongWritable val : values) {
			if (val > currMax) {
				currMax = val;
			}
		}

		maxViews.set(currMax);
	}

	public static class ViewsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		private Text time = new Text();
		private LongWritable views = new LongWritable();
		private Pattern wordSep = Pattern.compile("[\\s]+");

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = wordSep.split(value.toString());

			if (values[1] != "en" || values[2] == "Main_Page" || values[2].startsWith("Special:")) {
				return;
			}

			time.set(values[0]);
			views.set(values[3]);
			context.write(time, views);
		}
	}

	public static class MaxReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		private LongWritable maxViews = new LongPairWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
			getMaxViews(values, maxViews);
			context.write(key, maxViews);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "wikipedia views");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(ViewsMapper.class);
		job.setCombinerClass(MaxReducer.class);
		job.setReducerClass(MaxReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}