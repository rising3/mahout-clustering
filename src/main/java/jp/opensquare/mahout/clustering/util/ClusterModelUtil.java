package jp.opensquare.mahout.clustering.util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jp.opensquare.mahout.clustering.model.Cluster;
import jp.opensquare.mahout.clustering.model.Point;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.tools.ant.DirectoryScanner;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

public class ClusterModelUtil {
	static ObjectMapper om = null;

	ClusterModelUtil() {}
		
	public static void writeJson(Path name, Object obj) {
		Writer writer = null;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(name.toString()), "UTF-8"));
			getObjectMapper().writeValue(writer, obj);
			writer.close();
		} catch (IOException e) {
			System.out.println(e);
		}
		
	}
	
	public static String toJson(Object obj) throws Exception{
		return getObjectMapper().writeValueAsString(obj);
	}

	public static List<Cluster> readClusters(FileSystem fs, Configuration conf, Path path ) throws IOException {
		return readClusters(fs, conf, getFinalCluster(path), getClusteredPoints(path));
	}

	public static List<Cluster> readClusters(FileSystem fs, Configuration conf, Path clustersName, Path pointsName) throws IOException {		
		List<Cluster> list = new ArrayList<Cluster>();
		Map<Integer, Cluster> map = new HashMap<Integer, Cluster>();

		// read output clusters
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, clustersName, conf);
		IntWritable key = new IntWritable();
		ClusterWritable value = new ClusterWritable();
		while (reader.next(key, value)) {
			Integer c = key.get();
			Double cx = value.getValue().getCenter().get(0);
			Double cy = value.getValue().getCenter().get(1);
			Double rx = value.getValue().getRadius().get(0);
			Double ry = value.getValue().getRadius().get(1);
			Cluster cluster = new Cluster(c, cx, cy, rx, ry);
			map.put(c, cluster);
			list.add(cluster);
		}
		reader.close();
		
		readPoints(fs, pointsName, conf, map);
		return list;
	}

	static void readPoints(FileSystem fs, Path name, Configuration conf, Map<Integer, Cluster> clusters) throws IOException {
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, name, conf);
		IntWritable key = new IntWritable();
		WeightedVectorWritable value = new WeightedVectorWritable();
		while (reader.next(key, value)) {
			Double x = value.getVector().get(0);
			Double y = value.getVector().get(1);
			Cluster cluster = clusters.get(key.get());
			cluster.getPoints().add(new Point(x, y));
		}
		reader.close();
	}
	
	static ObjectMapper getObjectMapper() {
		if(om == null) {
			om = new ObjectMapper();
			om.enable(SerializationConfig.Feature.INDENT_OUTPUT);
		}
		return om;
	}
	
	public static Path getFinalCluster(Path path) {
		String  s[] = scanFiles(new Path(path, "clusters-*-final/part-*"));
		return new Path(s[0]);
	}

	public static Path getClusteredPoints(Path path) {
		String  s[] = scanFiles(new Path(path, Kluster.CLUSTERED_POINTS_DIR + "/part-*"));
		return new Path(s[0]);
	}
	
	static String[] scanFiles(Path path) {
		DirectoryScanner scanner = new DirectoryScanner();
		scanner.setIncludes(new String[]{ path.toString() });
		scanner.setBasedir(".");
		scanner.setCaseSensitive(false);
		scanner.scan();
		return scanner.getIncludedFiles();
	}

	public static void main(String ... arg) {
		System.out.println(getClusteredPoints(new Path("output")));
		System.out.println(getFinalCluster(new Path("output")));		
	}
}
