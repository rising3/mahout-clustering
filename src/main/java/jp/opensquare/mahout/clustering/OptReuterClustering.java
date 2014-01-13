package jp.opensquare.mahout.clustering;

import java.util.List;

import jp.opensquare.mahout.clustering.model.WordCluster;
import jp.opensquare.mahout.clustering.util.ClusterModelUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class OptReuterClustering {

	public static void main(String args[]) throws Exception {
		// input data
		Path inData = new Path("reuters-opt-vectors");
		Path vectorsFolder = new Path(inData, "tfidf-vectors");
		Path samples = new Path(vectorsFolder, "part-r-00000");

		// canopy
		Path canopy = new Path("reuters-opt-canopy");
		Path canopyClusters = new Path(canopy, "clusters-0-final");

		// output data
		Path outData = new Path("reuters-opt-output");
		Path outClustered = new Path(outData, Kluster.CLUSTERED_POINTS_DIR);
		Path outClusteredFile = new Path(outClustered, "part-m-00000");

		// use custom measure strategy
		DistanceMeasure measure = new ReuterDistanceMeasure();

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		// delete data
		HadoopUtil.delete(conf, canopy);
		HadoopUtil.delete(conf, outData);

		// Execute Clustering by Canopy method
		CanopyDriver.run(conf, samples, canopy, measure, 0.7, 0.5, false, 0, false);

		// Execute Clustering by K-Means method
	    KMeansDriver.run(conf, vectorsFolder, canopyClusters, outData, measure, 0.01, 20, true, 0.0, false);
		
		ClusterDumper.main(new String[] {
				"--input", new Path(outData, "clusters-*-final").toString(),
				"--output", new Path("reuters-opt-dump").toString(),
				"--pointsDir",  new Path(outData, "clusterdPoints").toString(),
				"--dictionary",  new Path(inData,"dictionary.file-0").toString(),
				"--dictionaryType",  "sequencefile",
				"--substring", "60",
				"-dm", measure.getClass().getName()
				});

		// result to JSON
		List<WordCluster> list = ClusterModelUtil.readWordClusters(
				fs,
				conf,
				outData,
				samples,
				new Path(inData, "dictionary.file-0"),
				measure);
		ClusterModelUtil.writeJson(fs, conf, new Path("reuters-opt-clusters.json"),list);
		
	}
}
