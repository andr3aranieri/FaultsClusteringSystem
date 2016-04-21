package faultsclusteringsystem.jobs.indexcreation.index;

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

public class IndexCreationReducer extends Reducer<Text, Text, Text, Text> {

	String documentTable = "";
	String primaryKey = "";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		this.documentTable = context.getConfiguration().get("documenttable");

		if (this.documentTable.toLowerCase().equals("volatiledocument")) {
			this.primaryKey = "idVolatileDocument";
		} else if (this.documentTable.toLowerCase().equals("volatiledocument")) {
			this.primaryKey = "idPersistentDocument";
		} else if (this.documentTable.toLowerCase().equals("tmpdocument")) {
			this.primaryKey = "idTmpDocument";
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		try {
			String vector = "";
			for (Text value : values) {
				vector += value + "-";
			}

			vector = vector.substring(0, vector.length() - 1);

			String updateQuery = "UPDATE " + this.documentTable + " SET vector='" + vector + "' WHERE " + this.primaryKey + "=" + key.toString();

			System.out.println(updateQuery + "\n");

			// write update query in output key;
			context.write(key,  new Text(updateQuery));
			;
		} catch (Exception ex) {
			StringWriter sw = new StringWriter();
			ex.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			context.write(new Text("ERRORE"),  new Text(exceptionAsString));
			System.err.println(exceptionAsString);
		}
	}
}
