package faultsclusteringsystem.jobs.silhouette;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

import faultsclusteringsystem.jobs.silhouette.pojo.ClusterAvgDistance;
import faultsclusteringsystem.jobs.silhouette.pojo.ClusterData;
import faultsclusteringsystem.jobs.silhouette.pojo.OutsideClusterMinDistance;
import faultsclusteringsystem.jobs.silhouette.pojo.PointData;
import faultsclusteringsystem.jobs.utility.MyVector;
import faultsclusteringsystem.manager.DocumentManager;

public class SilhouetteMapper extends Mapper<Text, Text, Text, Text> {

	private HashMap<String, ClusterAvgDistance> clusterAvgDistances = new HashMap<String, ClusterAvgDistance>();
	private HashMap<String, OutsideClusterMinDistance> outsideClusterMinDistance = new HashMap<String, OutsideClusterMinDistance>();

	private HashMap<String, Double> distances = null;
	private HashMap<String, String> clusteredDocuments = null;

	private List<String> documents = new ArrayList<String>();

	private DocumentManager documentManager = new DocumentManager();
	private int idUser = 0;

	@Override
	public void setup(Context context) {
		this.idUser = Integer.parseInt(context.getConfiguration().get("iduser"));
		try {
			this.distances = this.documentManager.getDistances(this.idUser);
			this.clusteredDocuments = this.documentManager.getTmpDocumentsClusters(idUser);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String docID = key.toString();
			this.documents.add(docID);
		} catch (Exception ex) {
			StringWriter sw = new StringWriter();
			ex.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			context.write(new Text("ERROR"), new Text(exceptionAsString));
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		String doc1 = "", doc2 = "";
		boolean sameCluster = false;
		ClusterAvgDistance cAvg = null;
		OutsideClusterMinDistance outMinDistance = null;
		Double distance = 0.0;
		String key = "", keyInv = "";
		String cluster1 = "", cluster2 = "";
		for (int i = 0; i < this.documents.size(); i++) {
			doc1 = this.documents.get(i);
			for (int j = 0; j <= this.documents.size() && j != i; j++) {
				doc2 = this.documents.get(j);

				key = doc1 + ":" + doc2;
				keyInv = doc2 + ":" + doc1;

				distance = this.distances.get(key);
				if (distance == null) {
					distance = this.distances.get(keyInv);
					if (distance == null) {
						distance = 0.0;
					}
				}

				cluster1 = this.clusteredDocuments.get(doc1);
				cluster2 = this.clusteredDocuments.get(doc2);
				
				if(cluster1 != null && cluster2 != null)
					sameCluster = cluster1.equals(cluster2);
				else
					sameCluster = false;

				if (sameCluster) {
					cAvg = this.clusterAvgDistances.get(doc1);
					if (cAvg == null) {
						cAvg = new ClusterAvgDistance();
					}

					cAvg.setSumDistance(cAvg.getSumDistance() + 0.0);
					cAvg.setNum(cAvg.getNum() + 1);

					this.clusterAvgDistances.put(doc1, cAvg);
				} else {
					outMinDistance = this.outsideClusterMinDistance.get(doc1);
					if (outMinDistance == null) {
						outMinDistance = new OutsideClusterMinDistance();
					}

					if (distance > 0.0 && outMinDistance.getDistance() >= distance) {
						outMinDistance.setDistance(distance);
					}
					
					this.outsideClusterMinDistance.put(doc1, outMinDistance);
				}
			}
		}
		
		Double[] a = new Double[this.documents.size()];
		Double[] b = new Double[this.documents.size()];
		
		String docID = "";
		Double silhouette = 0.0;
		ClusterAvgDistance inD = null;
		OutsideClusterMinDistance outD = null;
		for(int i = 0; i < this.documents.size(); i++) {
			docID = this.documents.get(i);
			inD = this.clusterAvgDistances.get(docID);
			outD = this.outsideClusterMinDistance.get(docID);
			if(inD != null && outD != null) {
				a[i] = ((double) inD.getSumDistance()) / inD.getNum();
				b[i] = outD.getDistance();
			}
			
			if((a[i] != null && a[i] > 0.0) || (b[i] != null && b[i] > 0.0))
				silhouette = ((double) b[i] - a[i]) / Math.max(a[i], b[i]);
			
			context.write(new Text(docID), new Text(silhouette.toString()));
		}
	}
}
