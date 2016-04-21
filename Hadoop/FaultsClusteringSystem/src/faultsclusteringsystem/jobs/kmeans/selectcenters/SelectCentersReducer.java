package faultsclusteringsystem.jobs.kmeans.selectcenters;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.*;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class SelectCentersReducer extends Reducer<Text, Text, Text, Text> {

		private TreeMap<Double, String> treeMapCenters = new TreeMap<Double, String>();
	
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
            	String sOut = "";
            	String aKeyValues[] = key.toString().split("\\|");
            	String sKey = aKeyValues[0];
            	Double weight = Double.parseDouble(aKeyValues[1]);
            	for(Text value: values) {
            		sOut += value.toString() + "-";
            	}
            	
            	sOut = sOut.substring(0, sOut.length()-1);

            	treeMapCenters.put(weight, sKey + "\t" + sOut);            	
            }            
            catch(Exception ex) {
        		StringWriter sw = new StringWriter();
        		ex.printStackTrace(new PrintWriter(sw));
        		String exceptionAsString = sw.toString();
                context.write(new Text("ERROR"), new Text(exceptionAsString));        		
            }                
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	Configuration conf = context.getConfiguration();
        	int k = Integer.parseInt(conf.get("k"));
        	
        	Iterator<Double> it = treeMapCenters.keySet().iterator();
        	Double dKey = 0.0;
        	HashMap<String, String> outputCenters = new HashMap<String, String>();
        	String[] aValues = null;
        	for(int i = 0; i < k; i++) {
        		dKey = it.next();
        		aValues = treeMapCenters.get(dKey).split("\t");
        		outputCenters.put(aValues[0], aValues[1]);
        	}
        	
        	this.writeInitialCentersToHDFS(context, outputCenters);
        }
        
        private void writeInitialCentersToHDFS(Context context, HashMap<String, String> centersMap) throws IOException, InterruptedException {
        	Configuration conf = context.getConfiguration();
    		Path centers = new Path(conf.get("centers.path") + "/centers");
    		FileSystem fs = FileSystem.get(conf);
    		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(centers,true)));
    		
        	Iterator<String> it = centersMap.keySet().iterator();
        	String sKey = "";
        	while (it.hasNext()) {
        		sKey = it.next();
            	//context.write(new Text(sKey), new Text(centersMap.get(sKey)));
        		br.write(centersMap.get(sKey) + "\n");
        	}
        	
        	br.close();
        }
}
