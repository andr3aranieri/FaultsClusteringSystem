package faultsclusteringsystem.jobs.indexcreation.tfidf;

import java.io.IOException;
import java.util.*;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
    
public class TfidfMappingReducer extends Reducer<Text, Text, Text, Text> {

		private HashMap<String, String> docsOccurrences = new HashMap<String, String>();
		private int docCount = 0;
	
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                String term = "";
                String docID = key.toString();
                int termFrequency = 0;
                String[] aValues = null;
                String docFrequency = "";
                String[] aDocFrequency = null;
                for (Text value : values) {                    
                    aValues = value.toString().split(":");
                    term = aValues[0];
                    termFrequency = Integer.parseInt(aValues[1]);
                    
                    docFrequency = this.docsOccurrences.get(term);
                    if(docFrequency == null)
                    	this.docsOccurrences.put(term, docID + ":" + termFrequency);
                    else {
                    	this.docsOccurrences.put(term, this.docsOccurrences.get(term) + "," + docID + ":" + termFrequency);
                    }
                }
                
                this.docCount += 1;
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
            try {
    		//computing tfidf;
            Iterator<String> iterator = this.docsOccurrences.keySet().iterator();                        
            String docID = "";
            double idf = 0;
            double tfidf = 0;
            String outRow = "";
            String term = "";
            String[] aDocs = null;
            String[] aDocValue = null;
            int docFrequency = 0;
            String docHashMapValue = "";
            int termFrequency = 0;
            HashMap<String, String> docHashMap = new HashMap<String, String>();
            while(iterator.hasNext()){
                term = iterator.next();
                
                aDocs = this.docsOccurrences.get(term).split(",");
                docFrequency = aDocs.length;
                for(String s: aDocs) {
                	aDocValue = s.split(":");
                	docID = aDocValue[0];
                	termFrequency = Integer.parseInt(aDocValue[1]);

                	idf = Math.log10(new Double(this.docCount) / new Double(docFrequency));                    
                    tfidf = new Double(termFrequency) * new Double(idf);
                	
                	docHashMapValue = docHashMap.get(docID);
                	if(docHashMapValue == null) {
                		docHashMap.put(docID, term + ":" + tfidf);
                	}
                	else {
                		docHashMap.put(docID, docHashMap.get(docID) + "," + term + ":" + tfidf);
                	}
                }
            }                                          

            Iterator<String> itDocHashMap = docHashMap.keySet().iterator();
            while(itDocHashMap.hasNext()) {
            	docID = itDocHashMap.next();
                context.write(new Text(docID), new Text(docHashMap.get(docID)));             	
            }
        }
        catch(Exception ex) {
    		StringWriter sw = new StringWriter();
   			ex.printStackTrace(new PrintWriter(sw));
   			String exceptionAsString = sw.toString();
   			context.write(new Text("ERROR"), new Text(exceptionAsString));
           }
     }
    	
    	private double getN2(String s) {
    		double n2 = 0;
    		String aTerms[] = s.split(",");
    		double tfidf = 0;
    		String tValues[] = null;
    		for (String st: aTerms) {
    			tValues = st.split(":");
    			tfidf = Double.parseDouble(tValues[1]);
    			n2 += Math.pow(tfidf, 2);
    		}
    		return Math.sqrt(n2);
    	}
}
