package faultsclusteringsystem.manager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

import faultsclusteringsystem.config.MyConfig;
import faultsclusteringsystem.db.TmpClusterDB;
import faultsclusteringsystem.db.VolatileClusterDB;
import faultsclusteringsystem.view.ClusteredDocument;

public class ClusterManager {

	private TmpClusterDB tmpClusterDB = new TmpClusterDB();
	private VolatileClusterDB volatileClusterDB = new VolatileClusterDB();

	public boolean deleteVolatileClusters(int idUser) throws ClassNotFoundException, SQLException {
		return this.volatileClusterDB.deleteClusters(idUser);
	}

	public boolean deleteTmpClusters(int idUser) throws ClassNotFoundException, SQLException {
		return this.tmpClusterDB.deleteClusters(idUser);
	}

	public File getTmpClusteredDocuments(int idUser) throws ClassNotFoundException, SQLException, IOException {
		String pathToUserDir = MyConfig.localFilesDirectory.concat(File.separator);
		File f = new File(pathToUserDir, idUser + "_todayclustereddocuments");
		if (!f.exists()) {
			f.createNewFile();
		}

		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(f));
		List<ClusteredDocument> docs = this.tmpClusterDB.getClusteredDocuments(idUser);
		for (ClusteredDocument doc : docs) {
			osw.write(doc.getIdDocument() + "," + doc.getVector() + "," + doc.getCenter() + "\n");
		}
		osw.close();
		return f;
	}

	public void writeTmpClusteredDocumentsToJobInputDirectory(int idUser, String inputDirectory) throws IOException, ClassNotFoundException, SQLException {
	 Configuration conf = new Configuration();
	    Path seqFilePath = new Path(inputDirectory + "/" + "inputClusteredDocuments.seq");
	    SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(seqFilePath), Writer.keyClass(Text.class), Writer.valueClass(Text.class));
		List<ClusteredDocument> docs = this.tmpClusterDB.getClusteredDocuments(idUser);
		for (ClusteredDocument doc : docs) {
			writer.append(new Text(doc.getCenter() + ""), new Text(doc.getVector() + "," + doc.getIdDocument()));
		}
	    writer.close();
	}
}
