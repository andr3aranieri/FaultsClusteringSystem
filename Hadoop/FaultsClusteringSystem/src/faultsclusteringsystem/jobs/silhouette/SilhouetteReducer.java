package faultsclusteringsystem.jobs.silhouette;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import faultsclusteringsystem.jobs.indexcreation.customoutputclasses.DBVolatileDocumentOutputWritable;

public class SilhouetteReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		try {
			String outString = "";
			for (Text value : values) {
				outString += value + " ";
			}

			context.write(key, new Text(outString));
			System.out.println(key.toString() + ", " + outString);
		} catch (Exception ex) {
			StringWriter sw = new StringWriter();
			ex.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			context.write(new Text("ERRORE"), new Text(exceptionAsString));
			System.err.println(exceptionAsString);
		}
	}
}
