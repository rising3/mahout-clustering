package jp.opensquare.mahout.clustering.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class ClusteringUtil {
	ClusteringUtil() {}
	
	public static void writeVectorsToFile(FileSystem fs, Configuration conf, List<Vector> vectors, Path path) throws IOException {
	    SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, VectorWritable.class);
        VectorWritable vec = new VectorWritable();
        for (Vector vector : vectors) {
          vec.set(vector);
          if(NamedVector.class.isAssignableFrom(vector.getClass())) {
        	  writer.append(new Text(((NamedVector)vector).getName()), vec);
          }
          else {
        	  writer.append(new Text(vector.toString()), vec);
          }
        }
        writer.close();
//		VectorDumper.main(new String[] { "--input", inPointFile.toString() });
//		SequenceFileDumper.main(new String[] { "--input", inPointFile.toString() });
	}

	public static void writePointsToFile(FileSystem fs, Configuration conf, List<Vector> points, Path path) throws IOException {
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, LongWritable.class, VectorWritable.class);
		long recNum = 0;
		VectorWritable vec = new VectorWritable();
		for (Vector point : points) {
			vec.set(point);
			writer.append(new LongWritable(recNum++), vec);
		}
		writer.close();

//		VectorDumper.main(new String[] { "--input", inPointFile.toString() });
//		SequenceFileDumper.main(new String[] { "--input", inPointFile.toString() });
	}

	public static void writeClustersToFile(FileSystem fs, Configuration conf, int k, List<Vector> points, Path path) throws IOException {
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, Kluster.class);
		for (int i = 0; i < k; i++) {
			Vector vec = points.get(i);
			Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
			writer.append(new Text(cluster.getIdentifier()), cluster);
		}
		writer.close();

//		SequenceFileDumper.main(new String[] { "--input", inClusterFile.toString() });		
	}
}
