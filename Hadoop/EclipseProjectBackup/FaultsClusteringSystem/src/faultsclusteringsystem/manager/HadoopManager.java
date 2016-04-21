package faultsclusteringsystem.manager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import faultsclusteringsystem.entity.User;
import faultsclusteringsystem.jobs.indexcreation.index.IndexCreationMapper;
import faultsclusteringsystem.jobs.indexcreation.index.IndexCreationReducer;
import faultsclusteringsystem.jobs.indexcreation.tfidf.TfidfMappingMapper;
import faultsclusteringsystem.jobs.indexcreation.tfidf.TfidfMappingReducer;
import faultsclusteringsystem.jobs.kmeans.iterations.ConvergenceMapper;
import faultsclusteringsystem.jobs.kmeans.iterations.ConvergenceReducer;
import faultsclusteringsystem.jobs.kmeans.selectcenters.SelectCentersMapper;
import faultsclusteringsystem.jobs.kmeans.selectcenters.SelectCentersReducer;
import faultsclusteringsystem.jobs.silhouette.SilhouetteMapper;
import faultsclusteringsystem.jobs.silhouette.SilhouetteReducer;
import faultsclusteringsystem.jobs.utility.MyVector;

public class HadoopManager {

	private DocumentManager documentManager = null;

	public HadoopManager() {
		this.documentManager = new DocumentManager();
	}

	public void writeFileToHDFS(String fileToWrite, String hdfsDirectory) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(0);
		}

		Path dir = new Path(hdfsDirectory);
		if (hdfs.isDirectory(dir))
			hdfs.copyFromLocalFile(new Path(fileToWrite), dir);
		else
			System.out.println("ERRORE HadoopManager.writeFileToHDFS()> dir:" + dir);
	}

	public void readFilesFromHDFSToDB(String hdfsDir)
			throws IOException, NumberFormatException, ClassNotFoundException, SQLException {
		// reading queries from sequence file to make a bulk update to DB;
		Configuration conf = new Configuration();
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(0);
		}

		FileStatus[] fss = hdfs.listStatus(new Path(hdfsDir), clusterFileFilter);
		List<String> updateQueries = new ArrayList<String>();
		for (FileStatus status : fss) {
			Path path = status.getPath();
			SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
			Text key = new Text();
			Text value = new Text();
			while (reader.next(key, value)) {
				updateQueries.add(value.toString());
			}
			reader.close();
		}

		this.documentManager.bulkUpdate(updateQueries);
	}

	public void readInsertsFromHDFSToDB(String hdfsDir)
			throws IOException, NumberFormatException, ClassNotFoundException, SQLException {
		// reading queries from sequence file to make a bulk update to DB;
		Configuration conf = new Configuration();
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(0);
		}

		FileStatus[] fss = hdfs.listStatus(new Path(hdfsDir), clusterFileFilter);
		List<String> insertQueries = new ArrayList<String>();
		for (FileStatus status : fss) {
			Path path = status.getPath();
			SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
			Text key = new Text();
			Text value = new Text();
			while (reader.next(key, value)) {
				if (!value.toString().endsWith("VALUES")) {
					insertQueries.add(value.toString());
				}
			}
			reader.close();
		}

		this.documentManager.bulkUpdate(insertQueries);
	}

	public void deleteHDFSDirectory(String hdfsDirectory) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(0);
		}

		Path dir = new Path(hdfsDirectory);
		if (hdfs.isDirectory(dir))
			hdfs.delete(new Path(hdfsDirectory), true);
	}

	public void tryToCreateDirectory(String hdfsDirectory) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(0);
		}

		Path dir = new Path(hdfsDirectory);
		if (!hdfs.exists(dir))
			hdfs.mkdirs(dir);
	}

	public List<MyVector> readCentersFromHDFS(Path centers, FileSystem fs) throws IOException, InterruptedException {
		List<MyVector> vectors = new ArrayList<MyVector>();

		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centers)));

		String sVector = "";
		int i = 0;
		while ((sVector = br.readLine()) != null) {
			if (sVector.trim().equals(""))
				break;

			vectors.add(new MyVector(sVector, "center_" + i));

			i += 1;
		}

		br.close();

		return vectors;
	}
	
	public void launchTfidfMappingJob(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(0);
		}

		Job job = Job.getInstance(conf, "TfidfMapping");
		job.setJarByClass(HadoopManager.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapperClass(TfidfMappingMapper.class);
		job.setReducerClass(TfidfMappingReducer.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}

	public void launchIndexCreationJob(String inputPath, String outputPath, String documentTable)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		conf.set("documenttable", documentTable);

		Job job = Job.getInstance(conf, "IndexCreation");
		job.setJarByClass(HadoopManager.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class); // reducer's KEYOUT
		job.setOutputValueClass(Text.class); // reducer's VALUEOUT

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapperClass(IndexCreationMapper.class);
		job.setReducerClass(IndexCreationReducer.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}

	public String launchKMeans(User user, String input, String output, String centersPath, String clustersTable)
			throws IOException, ClassNotFoundException, InterruptedException {

		String lastOutputDir = "";

		// Choose k centers;
		System.out.println("HP_KMeans> choose " + user.getK() + " centers...");
		Configuration conf = new Configuration();
		Path centers = new Path(centersPath);
		conf.set("centers.path", centers.toString());
		conf.set("k", user.getK() + "");
		Job job = Job.getInstance(conf, "CentersFirstSelection");
		job.setJarByClass(HadoopManager.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SelectCentersMapper.class);
		job.setReducerClass(SelectCentersReducer.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);

		// Iterate till convergence;
		System.out.println("HP_KMeans> iterate till convergence with treshold: " + user.getTreshold());
		int i = 1;
		while (true) {
			System.out.println("HP_KMeans> iteration " + i + ", output: " + output);
			lastOutputDir = output + "clusters_" + i;
			Job job2 = this.createConvergenceJob(user.getK(), user.getTreshold(), user.getDistanceMeasure(), input,
					lastOutputDir, centers, i, clustersTable, user.getIdUser());
			job2.waitForCompletion(true);

			long continua = job2.getCounters().findCounter(ConvergenceReducer.Counter.CONVERGED).getValue();

			if (continua == 1)
				break;

			i += 1;

			if (i >= 5) {
				System.out.println("HP_KMeans> can't converge... exit");
				break;
			}
		}

		System.out.println("HP_KMeans> end");

		return lastOutputDir;
	}

	public void launchSilhouetteJob(User user, String inputPath, String outputPath, String documentTable)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		conf.set("iduser", user.getIdUser() + "");
		
		Job job = Job.getInstance(conf, "SilhouetteEstimation");
		job.setJarByClass(HadoopManager.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapperClass(SilhouetteMapper.class);
		job.setReducerClass(SilhouetteReducer.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}

	private Job createConvergenceJob(int k, double treshold, String distanceMeasure, String input, String output,
			Path centers, int iteration, String clusterTable, int idUser) throws IOException {
		// Iterate till convergence;
		Configuration conf3 = new Configuration();
		conf3.set("centers.path", centers.toString());
		conf3.set("k", k + "");
		conf3.set("treshold", treshold + "");
		conf3.set("distanceMeasure", distanceMeasure);
		conf3.set("clusterstable", clusterTable);
		conf3.set("iduser", idUser + "");

		Job job3 = Job.getInstance(conf3, "ConvergenceJob_" + iteration);
		job3.setJarByClass(HadoopManager.class);
		job3.setJobName("IterateTillConvergence");

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		job3.setOutputFormatClass(SequenceFileOutputFormat.class);

		// first iteration: compute avg overall score for each beer;

		this.deleteHDFSDirectory(output);

		job3.setMapperClass(ConvergenceMapper.class);
		job3.setReducerClass(ConvergenceReducer.class);
		FileInputFormat.setInputPaths(job3, new Path(input));
		FileOutputFormat.setOutputPath(job3, new Path(output));
		return job3;
	}

	PathFilter clusterFileFilter = new PathFilter() {
		public boolean accept(Path path) {
			return path.getName().startsWith("part");
		}
	};
}
