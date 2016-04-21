package faultsclusteringsystem.jobs.kmeans.selectcenters;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;

public class SelectCentersMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
            	String[] aValue = value.toString().split("\t");
             	context.write(new Text(aValue[0] + "|" + Math.random()), new Text(aValue[1])); 
            }
            catch(Exception ex) {
        		StringWriter sw = new StringWriter();
        		ex.printStackTrace(new PrintWriter(sw));
        		String exceptionAsString = sw.toString();
                context.write(new Text("ERROR"), new Text(exceptionAsString));        		
            }
        }
}
