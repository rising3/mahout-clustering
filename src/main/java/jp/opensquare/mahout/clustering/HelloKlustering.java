package jp.opensquare.mahout.clustering;

import java.util.ArrayList;
import java.util.List;

import jp.opensquare.mahout.clustering.model.Cluster;
import jp.opensquare.mahout.clustering.util.ClusterModelUtil;
import jp.opensquare.mahout.clustering.util.ClusteringUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

public class HelloKlustering {
	

	public static void main(String args[]) throws Exception {
		// point data
		final double[][] points = {
			{ 1, 1 },
			{ 2, 1 },
			{ 1, 2 },
			{ 2, 2 },
			{ 3, 3 },
			{ 8, 8 },
			{ 9, 8 },
			{ 8, 9 },
			{ 9, 9 } };
		
		// input data
		Path inData = new Path("input");
		Path inPoints = new Path(inData, "points");
		Path inPointFile = new Path(inPoints, "file1");	
		Path inClusters = new Path(inData, "clusters");
		Path inClusterFile = new Path(inClusters, "part-m-00000");

		// output data
		Path outData = new Path("output");
		Path outClustered = new Path(outData, Kluster.CLUSTERED_POINTS_DIR);
		Path outClusteredFile = new Path(outClustered, "part-m-00000");

		// measure strategy
		DistanceMeasure measure = new EuclideanDistanceMeasure();

		// cluster size
		int k = 2;

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		// delete data
		HadoopUtil.delete(conf, inData);
		HadoopUtil.delete(conf, outData);

		// create point data
		List<Vector> vectors = getPoints(points);
		ClusteringUtil.writePointsToFile(fs, conf, vectors, inPointFile);
		
		// create cluster data		
		ClusteringUtil.writeClustersToFile(fs, conf, k, vectors, inClusterFile);

		// Execute Clustering by K-Means method
		KMeansDriver.run(conf, inPoints, inClusters, outData, measure, 0.001, 10, true, 0.0, false);

		// result to JSON
		List<Cluster> list = ClusterModelUtil.readClusters(fs, conf, outData);
		ClusterModelUtil.writeJson(new Path("clusters.json"),list);
		System.out.println(ClusterModelUtil.toJson(list));
//		ClusterDumper.main(new String[] { "--input", new Path("output/clusters-*-final").toString(), "--pointsDir",  new Path("output/clusterdPoints").toString() });
	}

	static List<Vector> getPoints(double[][] raw) {
		List<Vector> points = new ArrayList<Vector>();
		for (int i = 0; i < raw.length; i++) {
			double[] fr = raw[i];
			Vector vec = new RandomAccessSparseVector(fr.length);
			vec.assign(fr);
			points.add(vec);
		}
		return points;
	}
}
