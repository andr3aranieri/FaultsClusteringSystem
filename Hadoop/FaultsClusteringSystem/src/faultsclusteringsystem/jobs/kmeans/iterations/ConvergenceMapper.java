package faultsclusteringsystem.jobs.kmeans.iterations;

import faultsclusteringsystem.jobs.utility.MyVector;
import faultsclusteringsystem.manager.HadoopManager;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ConvergenceMapper extends Mapper<LongWritable, Text, Text, Text> {	
		private List<MyVector> centers;
		
		private int distanceMeasure; //1 = cosine, 2 = euclidean, 3 = manhattan;
		private final int COSINE = 1, EUCLIDEAN = 2, MANHATTAN = 3;
		
		private Configuration conf;
		private int k = 0;
		private HadoopManager hadoopManager = new HadoopManager();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			this.conf = context.getConfiguration();
	   		Path centersPath = new Path(conf.get("centers.path") + "/centers");
			//Path centersPath = new Path("hdfs://localhost:9000/Problem3/output/centers/centers.dat");
			FileSystem fs = FileSystem.get(conf);
	   	 	this.centers = this.hadoopManager.readCentersFromHDFS(centersPath, fs);
	   	 	//number of clusters;
        	this.k = Integer.parseInt(conf.get("k"));
			
	   	 	//writing a row for each center, to be sure to have exactly k clusters after this iteration;
	   	 	for (MyVector mv: this.centers) {
            	context.write(new Text(mv.getName()), new Text(mv.getName() + ":" + mv.toString()));	   	 		
	   	 	}
        	
			String dm = this.conf.get("distanceMeasure");
			if(dm.equals("cos"))
				this.distanceMeasure = COSINE;
			else if(dm.equals("euc"))
				this.distanceMeasure = EUCLIDEAN;
			else if(dm.equals("man"))
				this.distanceMeasure = MANHATTAN;
		}
	
		@Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {            
                String line = value.toString();
                String aValues[] = line.split("\t");
                String docID = "";
                String sVector = "";
                //pass to reduce class just the complete input lines;
                if(aValues.length >= 2) {
                	docID = aValues[0];
                	sVector = aValues[1];
                	MyVector vector = new MyVector(sVector, docID);
                	
                	double distance = 0.0;
                	TreeMap<Double, MyVector> treeMap = new TreeMap<Double, MyVector>();
                	for(MyVector center: this.centers) {
                		switch(this.distanceMeasure) {
                			case COSINE:
                				distance = vector.cosineDistance(center);
                				break;
                			case EUCLIDEAN:
                				distance = vector.euclideanDistance(center);
                				break;
                			case MANHATTAN:
                				distance = vector.manhattanDistance(center);
                				break;                				
                		}                		
        				treeMap.put(distance, center);
                	}
                	
                	MyVector nearestCenter = treeMap.get(treeMap.firstKey());
                	
                    context.write(new Text(nearestCenter.getName()), new Text(docID + ":" + sVector));
                }
            }
            catch(Exception ex) {
        		StringWriter sw = new StringWriter();
        		ex.printStackTrace(new PrintWriter(sw));
        		String exceptionAsString = sw.toString();
                context.write(new Text("ERROR"), new Text(exceptionAsString));        		
            }
        }
}
