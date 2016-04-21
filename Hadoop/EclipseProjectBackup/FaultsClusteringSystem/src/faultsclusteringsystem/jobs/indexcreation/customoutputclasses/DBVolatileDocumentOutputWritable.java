package faultsclusteringsystem.jobs.indexcreation.customoutputclasses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.sql.PreparedStatement;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.db.DBWritable;


public class DBVolatileDocumentOutputWritable implements WritableComparable, DBWritable
{
   private int idDocument;
   private int idUser;
   private String rawData;
   private String vector;
   private Date insertDate;

   public DBVolatileDocumentOutputWritable() {}
   
   public DBVolatileDocumentOutputWritable(int idDocument, String vector) {
     this.idDocument = idDocument;
     this.vector = vector;
   }

   public void readFields(DataInput in) throws IOException {   }

   public void readFields(ResultSet rs) throws SQLException {
     this.idDocument = rs.getInt(1);
     this.idUser = rs.getInt(2);
     this.rawData = rs.getString(3);
     this.vector = rs.getString(4);
     this.insertDate = rs.getDate(5);
   }

   @Override
   public void write(PreparedStatement ps) throws SQLException {
     ps.setInt(1, this.idDocument);
     ps.setString(2, this.vector);
   }

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
}
