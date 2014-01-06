package jp.opensquare.mahout.clustering;

import java.util.ArrayList;
import java.util.List;

import jp.opensquare.mahout.clustering.model.Cluster;
import jp.opensquare.mahout.clustering.util.ClusterModelUtil;
import jp.opensquare.mahout.clustering.util.ClusteringUtil;
import jp.opensquare.mahout.clustering.util.RandomPointsUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class RndPointsClustering {

	public static void main(String args[]) throws Exception {
		// input data
		Path inData = new Path("input");
		Path inPoints = new Path(inData, "points");
		Path inPointFile = new Path(inPoints, "file1");		

		// canopy result
		Path canopyResult = new Path("canpoy");
		Path canopyClusters = new Path(canopyResult, "clusters-0-final");

		// output data
		Path outData = new Path("output");

		// measure strategy
		DistanceMeasure measure = new EuclideanDistanceMeasure();

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		// delete data
		HadoopUtil.delete(conf, inData);
		HadoopUtil.delete(conf, canopyResult);
		HadoopUtil.delete(conf, outData);

		// create InputData
		List<Vector> vectors = getPoints();
		ClusteringUtil.writePointsToFile(fs, conf, vectors, inPointFile);

		// Execute Clustering by Canopy method
		CanopyDriver.run(conf, inPoints, canopyResult, measure, 3.0, 1.5, false, 0, false);

		// Execute Clustering by K-Means method
		KMeansDriver.run(conf, inPoints, canopyClusters, outData, measure, 0.001, 30, true, 0.0, false);

		// result to JSON
		List<Cluster> list = ClusterModelUtil.readClusters(fs, conf, outData);
		ClusterModelUtil.writeJson(new Path("clusters.json"),list);
		System.out.println(ClusterModelUtil.toJson(list));		
//		ClusterDumper.main(new String[] { "--input", new Path("output/clusters-*-final").toString(), "--pointsDir",  new Path("output/clusterdPoints").toString() });
	}
	
	static List<Vector> getPoints() {
		List<Vector> points = new ArrayList<Vector>();
		RandomPointsUtil.generateSamples(points, 400, 1, 1, 2);
		RandomPointsUtil.generateSamples(points, 300, 1, 0, 0.5);
		RandomPointsUtil.generateSamples(points, 300, 0, 2, 0.1);
		return points;
	}
}
