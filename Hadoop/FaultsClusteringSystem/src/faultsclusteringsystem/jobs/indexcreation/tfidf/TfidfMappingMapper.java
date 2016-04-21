package faultsclusteringsystem.jobs.indexcreation.tfidf;

import java.io.IOException;
import java.util.*;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TfidfMappingMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {            
                String line = value.toString();
                String aValues[] = line.split("\\|");
                Integer occurrences = 1;
                
                //pass to reduce class just the complete input lines;
                if(aValues.length >= 2) {
                    String docID = aValues[0];
                    String text = aValues[1];
                    StringTokenizer str = new StringTokenizer(text);
                    String term  = "";
                    //String termDigest = "";
                    HashMap<String, Integer> termsMap = new HashMap<String, Integer>();                    
                    while(str.hasMoreTokens()) {
                        term = str.nextToken();
                        //minimize index dimension using term digests;
                        //termDigest = this.getDigest(term);
                        occurrences = termsMap.get(term);
                        if(occurrences == null) {
                            termsMap.put(term, 1);
                        }
                        else {
                            termsMap.put(term, occurrences + 1);
                        }                                                
                    }
                    
                    Iterator<String> iterator = termsMap.keySet().iterator();                        
                    String mk = "";
                    while(iterator.hasNext()){
                        mk = iterator.next();
                        //context.write(new Text(mk), new Text(docID + "|" + termsMap.get(mk)));
                        context.write(new Text(docID), new Text(mk + ":" + termsMap.get(mk)));
                    }                           
                }
            }
            catch(Exception ex) {
    			StringWriter sw = new StringWriter();
    			ex.printStackTrace(new PrintWriter(sw));
    			String exceptionAsString = sw.toString();
    			context.write(new Text("ERROR"), new Text(exceptionAsString));
            }
        }
        
        private String getDigest(String term) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        	//MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        	//messageDigest.update(term.getBytes());
        	//byte[] byteData = messageDigest.digest();
        	
        	int intData = term.hashCode();
        	byte[] byteData = toBytes(intData);
        	//convert the byte to hex format method 1
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < byteData.length; i++) {
             sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
            }
        	
        	return sb.toString();
        }
        
        private byte[] toBytes(int i)
        {
          byte[] result = new byte[4];

          result[0] = (byte) (i >> 24);
          result[1] = (byte) (i >> 16);
          result[2] = (byte) (i >> 8);
          result[3] = (byte) (i /*>> 0*/);

          return result;
        }
        
}
