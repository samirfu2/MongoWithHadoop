package com.nubo.mongo;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class HaoopMapReduceToMongo{  

	public static class MongoReaderrMapper extends Mapper<LongWritable, Text, NullWritable,BSONWritable> {
		private BSONWritable reduceResult; 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Value:--> "+value.toString());

			/*
			 *  To test the mongo DB write, Harcoding the input {key:value} as one single document
			 *   {"name":"samir","age":33,"address":"Bangalore"}
			 * */
			BSONObject outDoc= new BasicDBObjectBuilder().start()
					.add("_id",value.hashCode()+Math.random())
					.add("name", "samir")
					.add("age", 25)
					.add("address", "Bangalore")
					.get();
			BSONWritable reduceResult=new BSONWritable(outDoc);
			//reduceResult.setDoc(outDoc);
			context.write(NullWritable.get(),reduceResult);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Load Data To MongoDB");

		//MongoConfigUtil.setInputFormat(job.getConfiguration(), MongoInputFormat.class); 
		MongoConfigUtil.setOutputFormat(job.getConfiguration(), MongoOutputFormat.class); 

		MongoConfigUtil.setInputURI(job.getConfiguration(), "mongodb://NN2.local:27017/nubo.element_data");
		MongoConfigUtil.setOutputURI(job.getConfiguration(), "mongodb://NN2.local:27017/nubo.element_data"); 
		//conf.set("mongo.output.uri","mongodb://NN2.local:27017/nubo.element_data");

		job.setJarByClass(HaoopMapReduceToMongo.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(MongoReaderrMapper.class);

		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		job.setOutputKeyClass(NullWritable.class);

		MongoConfigUtil.setOutputValue(job.getConfiguration(), BSONWritable.class); 
		FileInputFormat.addInputPath(job, new Path(args[0]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}