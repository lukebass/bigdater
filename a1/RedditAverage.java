import java.io.IOException;

import org.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RedditAverage extends Configured implements Tool {

	public static class ScoreMapper extends Mapper<LongWritable, Text, Text, LongPairWritable> {

		private LongPairWritable valuePair = new LongPairWritable();
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject record = new JSONObject(value.toString());
            word.set((String) record.get("subreddit"));
            valuePair.set(1, (Integer) record.get("score"));
			context.write(word, valuePair);
		}
    }

    public static class ScoreCombiner extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		private LongPairWritable valuePair = new LongPairWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int score = 0;
			for (LongPairWritable val : values) {
                count += val.get_0();
                score += val.get_1();
            }

			valuePair.set(count, score);
			context.write(key, valuePair);
		}
	}
    
    public static class AverageReducer extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int score = 0;
			for (LongPairWritable val : values) {
                count += val.get_0();
                score += val.get_1();
            }

			result.set(score/count);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(ScoreMapper.class);
		job.setCombinerClass(ScoreCombiner.class);
		job.setReducerClass(AverageReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}