package com.nubo.mongo;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class MongoToHadoopMapReduce {

	public static class MongoReaderrMapper 	extends Mapper<Object, BasicDBObject, Text, Text>{
		public void map(Object key, BasicDBObject value, Context context) throws IOException, InterruptedException {
			double _id = value.getDouble("_id");
			String description ="";
			if(value.containsField("description"))
				description=value.getString("description");
			context.write(new Text(""+_id),new Text(description));
		}
	}
	public static class GeneralReducer 	extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,	Context context) throws IOException, InterruptedException {
			Text result =  null;
			for (Text val : values) {
				result = val;
			}
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Mongodb fetch");

		MongoConfigUtil.setInputURI(job.getConfiguration(), "mongodb://NN2.local:27017/nubo.reading_type");
		MongoConfigUtil.setOutputURI(job.getConfiguration(), "mongodb://NN2.local:27017/nubo.reading_type"); 

		job.setJarByClass(MongoToHadoopMapReduce.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(MongoReaderrMapper.class);
		job.setCombinerClass(GeneralReducer.class);
		job.setReducerClass(GeneralReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(com.mongodb.hadoop.MongoInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}