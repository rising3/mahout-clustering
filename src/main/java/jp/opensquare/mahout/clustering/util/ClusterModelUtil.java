package jp.opensquare.mahout.clustering.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import jp.opensquare.mahout.clustering.model.WordFile;
import jp.opensquare.mahout.clustering.model.Point;
import jp.opensquare.mahout.clustering.model.PointCluster;
import jp.opensquare.mahout.clustering.model.WordCluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

public class ClusterModelUtil {
	static ObjectMapper om = null;

	ClusterModelUtil() {}
		
	public static void writeJson(FileSystem fs, Configuration conf, Path name, Object obj) {
		Writer writer = null;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(fs.create(name), "UTF-8"));
			getObjectMapper().writeValue(writer, obj);
			writer.close();
		} catch (IOException e) {
			System.out.println(e);
		}		
	}
	
	public static String toJson(Object obj) throws Exception{
		return getObjectMapper().writeValueAsString(obj);
	}

	public static List<PointCluster> readClusters(FileSystem fs, Configuration conf, Path path ) throws IOException {
		return readClusters(fs, conf, getFinalCluster(fs, conf, path), getClusteredPoints(fs, conf, path));
	}

	public static List<PointCluster> readClusters(FileSystem fs, Configuration conf, Path clustersName, Path pointsName) throws IOException {		
		List<PointCluster> list = new ArrayList<PointCluster>();
		Map<Integer, PointCluster> map = new HashMap<Integer, PointCluster>();

		// read output clusters
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, clustersName, conf);
		IntWritable key = new IntWritable();
		ClusterWritable value = new ClusterWritable();
		while (reader.next(key, value)) {
			Integer c = key.get();
			Long n = value.getValue().getNumObservations();
			Double cx = value.getValue().getCenter().get(0);
			Double cy = value.getValue().getCenter().get(1);
			Double rx = value.getValue().getRadius().get(0);
			Double ry = value.getValue().getRadius().get(1);
			PointCluster cluster = new PointCluster(c, n, cx, cy, rx, ry);
			map.put(c, cluster);
			list.add(cluster);
		}
		reader.close();
		readPoints(fs, conf, pointsName, map);
		return list;
	}

	public static List<WordCluster> readWordClusters(
			FileSystem fs,
			Configuration conf,
			Path path,
			Path vectorName,
			Path dictionaryName,
			DistanceMeasure measure) throws IOException {
		return readWordClusters(
				fs,
				conf,
				getFinalCluster(fs, conf, path),
				getClusteredPoints(fs, conf, path),
				vectorName,
				dictionaryName,
				measure);
	}

	public static List<WordCluster> readWordClusters(
			FileSystem fs,
			Configuration conf,
			Path clustersName,
			Path pointsName,
			Path vectorName,
			Path dictionaryName,
			DistanceMeasure measure) throws IOException {		
		List<WordCluster> list = new ArrayList<WordCluster>();
		Map<Integer, WordCluster> map = new HashMap<Integer, WordCluster>();
		Map<Integer, Vector> centers = new HashMap<Integer, Vector>();

		// read output clusters
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, clustersName, conf);
		IntWritable key = new IntWritable();
		ClusterWritable value = new ClusterWritable();
		while (reader.next(key, value)) {
			Integer c = key.get();
			Long n = value.getValue().getNumObservations();
			WordCluster cluster = new WordCluster(c, n);
			map.put(c, cluster);
			list.add(cluster);
			centers.put(c, value.getValue().getCenter());
		}
		reader.close();
		// read dictionary
		Map<Integer, String> dic = readDictionary(fs, conf, dictionaryName);
		// read word;
		readWords(fs, conf, pointsName, map, dic);
		// bind input file
		bindTFIDFVector(fs, conf, vectorName, map, centers, measure);
		return list;
	}

	static void readPoints(FileSystem fs, Configuration conf, Path name, Map<Integer, PointCluster> clusters) throws IOException {
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, name, conf);
		IntWritable key = new IntWritable();
		WeightedVectorWritable value = new WeightedVectorWritable();
		while (reader.next(key, value)) {
			Double x = value.getVector().get(0);
			Double y = value.getVector().get(1);
			PointCluster cluster = clusters.get(key.get());
			cluster.getPoints().add(new Point(x, y));
		}
		reader.close();
	}

	static void readWords(
			FileSystem fs,
			Configuration conf,
			Path name,
			Map<Integer, WordCluster> clusters,
			Map<Integer, String> dic) throws IOException {

		SequenceFile.Reader reader = new SequenceFile.Reader(fs, name, conf);
		IntWritable key = new IntWritable();
		WeightedVectorWritable value = new WeightedVectorWritable();
		while (reader.next(key, value)) {
			Iterator<Vector.Element> ite =value.getVector().iterateNonZero();
			WordCluster cluster = clusters.get(key.get());
			while(ite.hasNext()) {
				Vector.Element e = ite.next();
				Integer k = e.index();
				String t = dic.get(k);
				Double v = e.get();
				cluster.add(new jp.opensquare.mahout.clustering.model.Word(k, t, v));
			}
		}
		reader.close();
	}
	
	static void bindTFIDFVector(
			FileSystem fs,
			Configuration conf,
			Path name,
			Map<Integer,
			WordCluster> clusters,
			Map<Integer, Vector> centers,
			DistanceMeasure measure) throws IOException {

		// 各文書のVectorとClusterのCenterとの距離を見て、最も近いクラスタを選択する
		for (Pair<Writable, Writable> record : new SequenceFileIterable<Writable, Writable>(name, true, conf)) {
			String title = ((Text) record.getFirst()).toString();
			Vector vec = ((VectorWritable) record.getSecond()).get();

			double min = Double.MAX_VALUE;
			int c = -1;
			for (Map.Entry<Integer, Vector> entry : centers.entrySet()) {
				double d = measure.distance(entry.getValue(), vec);
				if (min > d) {
					min = d;
					c = entry.getKey();
				}
			}
			WordCluster cluster = clusters.get(c);
			cluster.getFiles().add(new WordFile(title, min));
		}
	}	
	
	public static Map<Integer, String> readDictionary(FileSystem fs, Configuration conf, Path name) throws IOException {		
		Map<Integer, String> dic = new HashMap<Integer, String>();

		// read dictionary
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, name, conf);
		Text key = new Text();
		IntWritable value = new IntWritable();
		while (reader.next(key, value)) {
			dic.put(value.get(), key.toString());
		}
		reader.close();
		return dic;
	}

	static ObjectMapper getObjectMapper() {
		if(om == null) {
			om = new ObjectMapper();
			om.enable(SerializationConfig.Feature.INDENT_OUTPUT);
		}
		return om;
	}
	
	public static Path getFinalCluster(FileSystem fs, Configuration conf, Path path) throws IOException {
		PathFilter filter = new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return (path.toString().endsWith("-final"));
			}
		};
		FileStatus fileStatus = fs.getFileStatus(path);
		FileStatus status[] = fs.listStatus(fileStatus.getPath(), filter);
		Path targetPath = (status.length == 1) ? status[0].getPath() : fileStatus.getPath();
		return new Path(targetPath, "part-r-00000");
	}

	public static Path getClusteredPoints(FileSystem fs, Configuration conf, Path path) throws IOException {
		PathFilter filter = new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.toString().endsWith(Kluster.CLUSTERED_POINTS_DIR);
			}
		};
		FileStatus fileStatus = fs.getFileStatus(path);
		FileStatus status[] = fs.listStatus(fileStatus.getPath(), filter);
		Path targetPath = (status.length == 1) ? status[0].getPath() : fileStatus.getPath();
		return new Path(targetPath, "part-m-00000");
	}
	
	public static void main(String ... arg) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
//		System.out.println(getClusteredPoints(fs, conf, new Path("reuters-output")));
//		System.out.println(getFinalCluster(fs, conf, new Path("reuters-output")));		
		List<WordCluster> list = readWordClusters(
				fs,
				conf,
				new Path("meigen-output"),
				new Path("meigen-vectors/tfidf-vectors/part-r-00000"),
				new Path("meigen-vectors/dictionary.file-0"),
				new CosineDistanceMeasure());
		System.out.println(toJson(list));

//		String vectorsFolder = "reuters-vectors/tfidf-vectors";
//		SequenceFile.Reader reader = new SequenceFile.Reader(
//				fs, new Path(vectorsFolder, "part-r-00000"), conf);
//
//		Text key = new Text();
//		VectorWritable value = new VectorWritable();
//		while (reader.next(key, value)) {
//			System.out.println(key.toString() + " = > "
//					+ value.get().asFormatString());
//		}
//		reader.close();
//		VectorDumper.main(new String[] { "--input", new Path(vectorsFolder, "part-r-00000").toString() });
//		SequenceFileDumper.main(new String[] { "--input", new Path(vectorsFolder, "part-r-00000").toString() });
//		SequenceFileDumper.main(new String[] { "--input", new Path("reuters-vectors/dictionary.file-0").toString() });
//		SequenceFileDumper.main(new String[] { "--input", new Path("reuters-vectors/frequency.file-0").toString() });	
	}
}
