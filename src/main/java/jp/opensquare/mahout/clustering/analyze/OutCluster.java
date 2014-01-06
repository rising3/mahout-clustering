package jp.opensquare.mahout.clustering.analyze;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class OutCluster {

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		DistanceMeasure measure = new CosineDistanceMeasure();

		// Clusterの情報をMapに入れる
		Path clusterPath = new Path("meigen-kmeans/clusters-3-final/part-r-00000");
		Map<Integer, Vector> map = new HashMap<Integer, Vector>();
		for (Pair<Writable, Writable> record : new SequenceFileIterable<Writable, Writable>(
				clusterPath, true, conf)) {
			int clusterId = ((IntWritable) record.getFirst()).get();
			ClusterWritable value = (ClusterWritable) record.getSecond();
			map.put(clusterId, value.getValue().getCenter());
		}

		// 各文書のVectorとClusterのCenterとの距離を見て、最も近いクラスタを表示
		Path vectorPath = new Path("meigen-sparse/tfidf-vectors/part-r-00000");
		for (Pair<Writable, Writable> record : new SequenceFileIterable<Writable, Writable>(
				vectorPath, true, conf)) {
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
