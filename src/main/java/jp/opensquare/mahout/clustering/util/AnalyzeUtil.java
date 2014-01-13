package jp.opensquare.mahout.clustering.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class AnalyzeUtil {
	AnalyzeUtil() {}

	public static void dumpInnerClusterDistance(
			FileSystem fs,
			Configuration conf,
			Path clusterPath,
			DistanceMeasure measure) throws Exception {

		Path path = ClusterModelUtil.getFinalCluster(fs, conf, clusterPath);

		List<Cluster> clusters = new ArrayList<Cluster>();
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		Writable key = (Writable) reader.getKeyClass().newInstance();
		ClusterWritable value = (ClusterWritable) reader.getValueClass().newInstance();
		while (reader.next(key, value)) {
			clusters.add(value.getValue());
		}
		reader.close();

		double max = 0;
		double min = Double.MAX_VALUE;
		double sum = 0;
		int count = 0;
		for (int i = 0; i < clusters.size(); i++) {
			for (int j = i + 1; j < clusters.size(); j++) {
				double d = measure.distance(clusters.get(i).getCenter(), clusters.get(j).getCenter());
				min = Math.min(d, min);
				max = Math.max(d, max);
				sum += d;
				count++;
			}
		}
		System.out.println("Maximum Intercluster Distance: " + max);
		System.out.println("Minimum Intercluster Distance: " + min);
		System.out.println("Average Intercluster Distance(Scaled): " + (sum / count - min) / (max - min));
	}
	
	public static void dumpClusterMappdFile(
			FileSystem fs,
			Configuration conf,
			Path clusterPath,
			Path vectorName,
			DistanceMeasure measure) throws Exception {

		Path path = ClusterModelUtil.getFinalCluster(fs, conf, clusterPath);
		Map<Integer, Vector> map = new HashMap<Integer, Vector>();
		for (Pair<Writable, Writable> record : new SequenceFileIterable<Writable, Writable>(path, true, conf)) {
			int clusterId = ((IntWritable) record.getFirst()).get();
			ClusterWritable value = (ClusterWritable) record.getSecond();
			map.put(clusterId, value.getValue().getCenter());
		}

		for (Pair<Writable, Writable> record : new SequenceFileIterable<Writable, Writable>(vectorName, true, conf)) {
			String title = ((Text) record.getFirst()).toString();
			Vector vec = ((VectorWritable) record.getSecond()).get();

			double min = Double.MAX_VALUE;
			int cluster = -1;
			for (Map.Entry<Integer, Vector> entry : map.entrySet()) {
				double d = measure.distance(entry.getValue(), vec);
				if (min > d) {
					min = d;
					cluster = entry.getKey();
				}
			}
			System.out.println(cluster + " " + title + " : d=" + min);
		}		
	}
}
