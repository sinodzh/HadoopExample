package com.first;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountEx {

	static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			// 分割字符串
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				// 排除字母少于5个的
				String tmp = itr.nextToken();
				if (tmp.length() < 5)
					continue;
				word.set(tmp);
				context.write(word, one);
			}
		}

	}

	static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		private Text keyEx = new Text();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				// 将map的结果放大，乘以2
				sum += val.get() * 2;
			}
			result.set(sum);
			// 自定义输出key
			keyEx.set("输出:" + key.toString());
			context.write(keyEx, result);
		}

	}

	public static void main(String[] args) throws Exception {

		//配置信息
		Configuration conf = new Configuration();
		
		//job名称
		Job job = Job.getInstance(conf, "mywordcount");
		
		job.setJarByClass(WordCountEx.class);
		job.setMapperClass(MyMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(MyReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//输入、输出path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//结束
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
