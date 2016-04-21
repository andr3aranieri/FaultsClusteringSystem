package faultsclusteringsystem.jobs.kmeans.iterations;

import faultsclusteringsystem.jobs.utility.MyVector;
import faultsclusteringsystem.manager.HadoopManager;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ConvergenceReducer extends Reducer<Text, Text, Text, Text> {
	public static enum Counter {
		CONVERGED
	}

	private List<MyVector> centers;
	private int distanceMeasure; // 1 = cosine, 2 = euclidean, 3 = manhattan;
	private final int COSINE = 1, EUCLIDEAN = 2, MANHATTAN = 3;
	private Configuration conf;
	private HashMap<String, List<MyVector>> clusters = new HashMap<String, List<MyVector>>();
	private double threshold;
	private HadoopManager hadoopManager = new HadoopManager();
	private String clustersTable = "";
	private String idUser = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		try {
		this.conf = context.getConfiguration();
		Path centersPath = new Path(conf.get("centers.path") + "/centers");
		FileSystem fs = FileSystem.get(conf);
		this.centers = this.hadoopManager.readCentersFromHDFS(centersPath, fs);
		this.threshold = Double.parseDouble(conf.get("treshold"));						
		
		this.clustersTable = conf.get("clusterstable");
		this.idUser = conf.get("iduser");
		
		String dm = this.conf.get("distanceMeasure");
		if (dm.equals("cos"))
			this.distanceMeasure = COSINE;
		else if (dm.equals("euc"))
			this.distanceMeasure = EUCLIDEAN;
		else if (dm.equals("man"))
			this.distanceMeasure = MANHATTAN;
		}
		catch(Exception ex) {
			StringWriter sw = new StringWriter();
			ex.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			context.write(new Text("ERROR"), new Text(exceptionAsString));			
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		try {
			String outRow = "";
			String aValues[] = null;
			String docID = "";
			String sVector = "";
			MyVector vector = null;
			String insertQuery = "INSERT INTO " + this.clustersTable + " (idUser, idDocument, description, center) VALUES ";
			List<MyVector> clusterVectors = null;
			boolean documentsInCluster = false;
			for (Text value : values) {
				// value = docID:x1-x2-x3-...-xN
				// we save vectors of documents in the cluster in the cluster
				// hashmap entry, to rebuild centers;
				aValues = value.toString().split(":");
				docID = aValues[0];
				sVector = aValues[1];
				vector = new MyVector(sVector, docID, "-");

				//outRow += docID + ":" + vector.toString() + " ";
				if(!docID.trim().toLowerCase().equals(key.toString().trim().toLowerCase())) {
					outRow += docID + " ";
					insertQuery += "(" + this.idUser + ", " + docID + ", '" + key.toString() + "', '" + key.toString() + "'), ";
					documentsInCluster = true;
				}

				clusterVectors = this.clusters.get(key.toString());
				if (clusterVectors == null) {
					clusterVectors = new ArrayList<MyVector>();
					this.clusters.put(key.toString(), clusterVectors);
				}

				this.clusters.get(key.toString()).add(vector);				
			}
			
			if(documentsInCluster) {
				insertQuery = insertQuery.substring(0, insertQuery.length() - 2);			
				//context.write(key, new Text(outRow));
				context.write(key, new Text(insertQuery));
			}
		} catch (Exception ex) {
			StringWriter sw = new StringWriter();
			ex.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			context.write(new Text("ERROR"), new Text(exceptionAsString));
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		try {

			Iterator<String> it = this.clusters.keySet().iterator();
			List<MyVector> vectors = null;
			String clusterID = "";
			Double[] vArray = null; // it will contain the sum of the centers
									// coordinates;
			int dimension = 0;
			List<MyVector> newCenters = new ArrayList<MyVector>();
			MyVector newCenter = null;
			int numDocsInCluster = 0;
			while (it.hasNext()) {
				clusterID = it.next();
				vectors = this.clusters.get(clusterID);
				numDocsInCluster = vectors.size();
				for (MyVector v : vectors) {
					if (vArray == null) {
						dimension = v.getInnerVector().length;
						vArray = new Double[dimension];
						for (int i = 0; i < v.getInnerVector().length; i++) {
							vArray[i] = 0.0;
						}
					}

					// compute sums of all centers coordinates;
					for (int i = 0; i < v.getInnerVector().length; i++) {
						vArray[i] = vArray[i] + v.getInnerVector()[i];
					}					
				}

				// compute clusterID new center;
				for (int i = 0; i < dimension; i++) {
					vArray[i] = new Double(((double) vArray[i] / numDocsInCluster));					
				}

				newCenter = new MyVector(vArray, clusterID);

				newCenters.add(newCenter);

				vArray = null; //reinitialize for next cluster;			
			}
			
			writeInitialCentersToHDFS(context, newCenters);
			
			boolean convergence = false;
			double dist = 0.0;
			for(int i = 0; i < this.centers.size(); i++) {
				switch(this.distanceMeasure) {
					case COSINE:
						dist = this.centers.get(i).cosineDistance(newCenters.get(i));
						break;
					case EUCLIDEAN:
						dist = this.centers.get(i).euclideanDistance(newCenters.get(i));
						break;
					case MANHATTAN:
						dist = this.centers.get(i).manhattanDistance(newCenters.get(i));
						break;						
				}
				
				if(dist > this.threshold) {
					convergence = false;
					break;
				}
				else
					convergence = true;
			}
			
			
			// condizione di convergenza al di sotto della soglia raggiunta;
			if (convergence) 
				context.getCounter(Counter.CONVERGED).setValue(1);
			else
				context.getCounter(Counter.CONVERGED).setValue(0);
			
		} catch (Exception ex) {
			StringWriter sw = new StringWriter();
			ex.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			context.write(new Text("ERROR"), new Text(exceptionAsString));
		}		
	}

	private void writeInitialCentersToHDFS(Context context, List<MyVector> newCenters) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Path centers = new Path(conf.get("centers.path") + "/centers");
		FileSystem fs = FileSystem.get(conf);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(centers, true)));
		int i = 0;
		for(MyVector v: newCenters) {
			br.write(v.toString() + "\n");
			i += 1;
		}

		br.close();
	}

}
