package jp.opensquare.mahout.clustering.analyze;

import jp.opensquare.mahout.clustering.util.AnalyzeUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;

public class AnalyzeReuterMappedFile {
	public static void main(String[] args) throws Exception {
		DistanceMeasure measure = new CosineDistanceMeasure();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path vectorName = new Path("reuters-vectors/tfidf-vectors/part-r-00000");
		Path outData = new Path("reuters-output");

		AnalyzeUtil.dumpClusterMappdFile(fs, conf, outData, vectorName, measure);		
	}
}
