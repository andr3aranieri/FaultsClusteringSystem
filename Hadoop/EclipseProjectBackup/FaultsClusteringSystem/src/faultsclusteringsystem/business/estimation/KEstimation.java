package faultsclusteringsystem.business.estimation;

import faultsclusteringsystem.manager.DocumentManager;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import faultsclusteringsystem.entity.Distance;
import faultsclusteringsystem.entity.TmpDocument;
import faultsclusteringsystem.entity.User;
import faultsclusteringsystem.entity.VolatileDocument;
import faultsclusteringsystem.jobs.utility.MyVector;

public class KEstimation {

	private DocumentManager documentManager = new DocumentManager();
	
	public void computeDistances(User user) throws ClassNotFoundException, SQLException {
		List<Distance> distances = new ArrayList<Distance>();
		List<TmpDocument> documents = this.documentManager.getTmpDocuments(user.getIdUser());
		Distance d = null;
		HashMap<String, Boolean> computed = new HashMap<String, Boolean>();
		TmpDocument d1 = null;
		TmpDocument d2 = null;
		MyVector v1 = null;		
		MyVector v2 = null;
		
		Boolean c = false;
		Boolean cInv = false;
		
		System.out.println("KEstimation.DistanceMeasureThread.computeDistances()> start compute distances");

		for (int i = 0; i < documents.size(); i++) {
			d1 = documents.get(i);
			if(d1.getVector().trim().equals("")) {
				continue;
			}
			v1 = new MyVector(d1.getVector(), d1.getIdTmpDocument() + "");
			for(int j = 0; j < documents.size() && j != i; j++) {
				d2 = documents.get(j);
				if(d2.getVector().trim().equals("")) {
					continue;
				}
				
				c = computed.get(d1.getIdTmpDocument() + ":" + d2.getIdTmpDocument());
				cInv = computed.get(d2.getIdTmpDocument() + ":" + d1.getIdTmpDocument());
				
				if((c != null && c) || (cInv != null && cInv)) { //already computed this distance;
					System.out.println("ComputeDistances: already computed this distance> " + d1.getIdTmpDocument() + ":" + d2.getIdTmpDocument() + "...continue");
					continue;
				}
				
				v2 = new MyVector(d2.getVector(), d2.getIdTmpDocument() + "");
				d = new Distance();
				d.setIdUser(user.getIdUser());
				d.setPoint1(d1.getIdTmpDocument());
				d.setPoint2(d2.getIdTmpDocument());
				if(user.getDistanceMeasure().equals("cos")) {
					d.setDistance(v1.cosineDistance(v2));
				}
				else if(user.getDistanceMeasure().equals("euc")) {
					d.setDistance(v1.euclideanDistance(v2));					
				}
				else if(user.getDistanceMeasure().equals("man")) {
					d.setDistance(v1.manhattanDistance(v2));					
				} else {
					d.setDistance(0.0);					
				}
				d.setRead(false);
				
				computed.put(d.getPoint1() + ":" + d.getPoint2(), true);
				
				distances.add(d);
			}
		}
	
		System.out.println("KEstimation.DistanceMeasureThread.computeDistances()> write distances to DB");
		//bulk insert distances;
		this.documentManager.insertDistances(user.getIdUser(), distances);
	}
	
	
}
