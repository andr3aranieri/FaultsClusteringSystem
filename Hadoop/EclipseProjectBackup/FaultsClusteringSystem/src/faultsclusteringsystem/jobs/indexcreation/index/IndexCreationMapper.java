package faultsclusteringsystem.jobs.indexcreation.index;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexCreationMapper extends Mapper<Text, Text, Text, Text> {

	private TreeMap<String, String> termsIfIdf = new TreeMap<String, String>();

	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String docID = "";
		String terms = "";

		String[] aTerms = null;
		String[] aTermValues = null;
		String term = "";
		String[] aLineChunks = null;
		Double tfidf = 0.0;
		try {
			if(value.toString().trim().equals("")) 
				return;
			
			/*
			aLineChunks = value.toString().split("\t");
			docID = aLineChunks[0];
			terms = aLineChunks[1];
			*/
			
			docID = key.toString();
			terms = value.toString();
			
			aTerms = terms.split(",");
			String outputDocs = "";
			for (String t : aTerms) {
				aTermValues = t.split(":");
				if (aTermValues.length != 2) {
					continue;
				}
				term = aTermValues[0];
				tfidf = Double.parseDouble(aTermValues[1]);

				outputDocs = this.termsIfIdf.get(term);
				if (outputDocs == null)
					outputDocs = docID + ":" + tfidf;
				else
					outputDocs += "," + docID + ":" + tfidf;

				this.termsIfIdf.put(term, outputDocs);
			}
			// end map method;
		} catch (Exception ex)
		{
    		StringWriter sw = new StringWriter();
    		ex.printStackTrace(new PrintWriter(sw));
    		String exceptionAsString = sw.toString();
            context.write(new Text("ERROR"), new Text(exceptionAsString));        		
		}
	}
	
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
		int spaceDimension = this.termsIfIdf.keySet().size();
		
		String docs = "";
		String term = "";
		
		String aDocs[] = null;
		String sDoc = "";
		
		String[] docValues = null;
		String docName = "";
		String sTfidf = "";
		
		ArrayList<String> outputArray = null;
		HashMap<String, List<String>> outputMap = new HashMap<String, List<String>>();
		List<String> lTermsTfIdf = new ArrayList<String>(this.termsIfIdf.keySet());
		Iterator<String> it = this.termsIfIdf.keySet().iterator();
		while (it.hasNext()) {
			term = it.next();
			docs = this.termsIfIdf.get(term);
			aDocs = docs.split(",");
			for (String s: aDocs) {
				docValues = s.split(":");
				docName = docValues[0];
				sTfidf = docValues[1];
				
				outputArray = (ArrayList<String>) outputMap.get(docName);
				if(outputArray == null) {
					outputArray = (ArrayList<String>) this.createEmptyVector(spaceDimension);
				}
				outputArray.set(lTermsTfIdf.indexOf(term), sTfidf);
				outputMap.put(docName, outputArray);
			}			
		}
				
		String k = "";
		Iterator<String> iter = outputMap.keySet().iterator();
		List<String> oArray = null;
				
		while (iter.hasNext()) {
			k = iter.next();
			oArray = (ArrayList<String>) outputMap.get(k);
			for(String s: oArray) {
				context.write(new Text(k), new Text(s));
			}
		}    	
    }
    
	private List<String> createEmptyVector(int dimension) {
		List<String> ret = new ArrayList<String>(dimension);
		for (int i = 0; i < dimension; i++) {
			ret.add("0.0");
		}
		return ret;
	}
}
