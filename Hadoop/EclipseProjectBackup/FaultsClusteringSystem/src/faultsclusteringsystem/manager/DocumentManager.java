package faultsclusteringsystem.manager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Writer;

import faultsclusteringsystem.config.MyConfig;
import faultsclusteringsystem.db.DistanceDB;
import faultsclusteringsystem.db.GenericDB;
import faultsclusteringsystem.db.PersistentDocumentDB;
import faultsclusteringsystem.db.TmpDocumentDB;
import faultsclusteringsystem.db.VolatileDocumentDB;
import faultsclusteringsystem.entity.Distance;
import faultsclusteringsystem.entity.PersistentDocument;
import faultsclusteringsystem.entity.TmpDocument;
import faultsclusteringsystem.entity.VolatileDocument;
import faultsclusteringsystem.view.ClusteredDocument;

public class DocumentManager {
	
	private VolatileDocumentDB volatileDocumentDB = new VolatileDocumentDB();
	private PersistentDocumentDB persistentDocumentDB = new PersistentDocumentDB();
	private TmpDocumentDB tmpDocumentDB = new TmpDocumentDB();
	private DistanceDB distanceDB = new DistanceDB();
	private GenericDB genericDB = new GenericDB();
	
	public List<VolatileDocument> getVolatileDocuments(int idUser) throws ClassNotFoundException, SQLException {
		return this.volatileDocumentDB.getVolatileDocuments(idUser);
	}

	public List<TmpDocument> getTmpDocuments(int idUser) throws ClassNotFoundException, SQLException {
		return this.tmpDocumentDB.getTmpDocuments(idUser);
	}

	public HashMap<String, String> getVolatileDocumentsClusters(int idUser) throws ClassNotFoundException, SQLException {
		return this.volatileDocumentDB.getVolatileDocumentsClusters(idUser);
	}

	public HashMap<String, String> getTmpDocumentsClusters(int idUser) throws ClassNotFoundException, SQLException {
		return this.tmpDocumentDB.getTmpDocumentsClusters(idUser);
	}

	public List<PersistentDocument> getPersistentDocuments(int idUser, Date from, Date to) throws ClassNotFoundException, SQLException {
		return this.persistentDocumentDB.getVolatileDocuments(idUser, from, to);
	}
	
	public File getTodayDocuments(int idUser) throws ClassNotFoundException, SQLException, IOException {
		String pathToUserDir = MyConfig.localFilesDirectory.concat(File.separator);
		File f = new File(pathToUserDir, idUser + "_todaydocuments");
		if (!f.exists()) {
			f.createNewFile();
		}

		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(f));
		List<VolatileDocument> docs = this.getVolatileDocuments(idUser);
		for (VolatileDocument doc : docs) {
			osw.write(doc.getIdVolatileDocument() + "|" + doc.getRawData() + "\n");
		}
		osw.close();
		return f;
	}

	public File getTodayVectors(int idUser) throws ClassNotFoundException, SQLException, IOException {
		String pathToUserDir = MyConfig.localFilesDirectory.concat(File.separator);
		File f = new File(pathToUserDir, idUser + "_todaydocuments");
		if (!f.exists()) {
			f.createNewFile();
		}

		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(f));
		List<VolatileDocument> docs = this.getVolatileDocuments(idUser);
		for (VolatileDocument doc : docs) {
			osw.write(doc.getIdVolatileDocument() + "\t" + doc.getVector() + "\n");
		}
		osw.close();
		return f;
	}

	public File getTodayTmpVectors(int idUser) throws ClassNotFoundException, SQLException, IOException {
		String pathToUserDir = MyConfig.localFilesDirectory.concat(File.separator);
		File f = new File(pathToUserDir, idUser + "_tmpdocuments");
		if (!f.exists()) {
			f.createNewFile();
		}

		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(f));
		List<TmpDocument> docs = this.getTmpDocuments(idUser);
		for (TmpDocument doc : docs) {
			osw.write(doc.getIdTmpDocument() + "\t" + doc.getVector() + "\n");
		}
		osw.close();
		return f;
	}

	public void updateVolatileDocumentVector(int idDocument, String vector) throws ClassNotFoundException, SQLException{		
		this.volatileDocumentDB.updateDocumentVector(idDocument, vector);
	}
	
	public boolean bulkUpdate(List<String> queries) throws SQLException {
		return this.genericDB.bulkUpdate(queries);
	}

	public HashMap<String, Double> getDistances(int idUser) throws ClassNotFoundException, SQLException {
		return this.distanceDB.getDistances(idUser);
	}
	
	public boolean insertDistances(int idUser, List<Distance> distances) throws ClassNotFoundException, SQLException {
		boolean ret = true;
		String bulkInsertQuery = this.createDistancesBulkInsertQuery(distances);
		this.distanceDB.insertDistances(idUser, bulkInsertQuery);
		return ret;
	}
	
	/*
	public void writeVolatileDocumentsToJobInputDirectory(int idUser, String inputDirectory) throws IOException, ClassNotFoundException, SQLException {
		 Configuration conf = new Configuration();
		    Path seqFilePath = new Path(inputDirectory + "/" + "inputClusteredDocuments.seq");
		    SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(seqFilePath), Writer.keyClass(Text.class), Writer.valueClass(Text.class));
			List<VolatileDocument> docs = this.volatileDocumentDB.getVolatileDocuments(idUser);
			for (VolatileDocument doc : docs) {
				writer.append(new Text(doc.getIdVolatileDocument() + ""), new Text(""));
			}
		    writer.close();
	}
	 */
	
	public void writeTmpDocumentsToJobInputDirectory(int idUser, String inputDirectory) throws IOException, ClassNotFoundException, SQLException {
		 Configuration conf = new Configuration();
		    Path seqFilePath = new Path(inputDirectory + "/" + "inputDocuments.seq");
		    SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(seqFilePath), Writer.keyClass(Text.class), Writer.valueClass(Text.class));
			List<TmpDocument> docs = this.tmpDocumentDB.getTmpDocuments(idUser);
			for (TmpDocument doc : docs) {
				writer.append(new Text(doc.getIdTmpDocument() + ""), new Text(""));
			}
		    writer.close();
	}

	public void copyVolatileDocumentInTmpDocuments(int idUser) throws SQLException {
		this.tmpDocumentDB.copyVolatileDocuments(idUser);
	}
	
	private String createDistancesBulkInsertQuery(List<Distance> distances) {
		String query = "INSERT INTO Distance(idUser, distance, hasBeenRead, point1, point2) VALUES";
		
		for(Distance d: distances) {
			query += "(" + d.getIdUser()  + ", " + d.getDistance() + ", 0, " + d.getPoint1() + ", " + d.getPoint2() + "),";
		}
		
		query = query.substring(0, query.length() - 1);
		return query;
	}
}
