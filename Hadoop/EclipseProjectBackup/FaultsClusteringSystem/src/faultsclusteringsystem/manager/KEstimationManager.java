package faultsclusteringsystem.manager;

import faultsclusteringsystem.entity.VolatileDocument;
import faultsclusteringsystem.jobs.utility.MyVector;

import java.util.List;

public class KEstimationManager {

	public int getInitialKEstimation(List<VolatileDocument> documents) {		
		int M = documents.size(); //num documents
		int N = 0; //num terms
		int T = 0; //num non-zero entries
		
		MyVector vector = null;
		for(VolatileDocument vd: documents) {
			vector = new MyVector(vd.getVector(), vd.getIdVolatileDocument() + "");
			if(N == 0) {
				N = vector.getInnerVector().length;
			}
			
			for(double d: vector.getInnerVector()) {
				if (d > 0) {
					T += 1;
				}
			}
		}
		
		return (M * N) / T;
	}
}
