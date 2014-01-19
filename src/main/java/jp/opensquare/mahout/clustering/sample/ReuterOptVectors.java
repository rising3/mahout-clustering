package jp.opensquare.mahout.clustering.sample;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.SequenceFileDumper;
import org.apache.mahout.utils.vectors.VectorDumper;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

public class ReuterOptVectors {

	public static void main(String args[]) throws Exception {

		// 単語の最小出現回数
		int minSupport = 5;
		// 最小ドキュメント頻度
		int minDf = 5;
		// ドキュメント頻度の最大パーセンテージ
		int maxDFPercent = 90;
		// Nグラムの大きさ
		int maxNGramSize = 2;
		// 対数尤度比の最小値
		float minLLRValue = 50F;
		// Reducerの数
		int reduceTasks = 1;
		// チャンクサイズ
		int chunkSize = 200;
		int norm = -1;

		boolean sequentialAccessOutput = true;

		String inputDir = "reuters-seqfiles";
		String outputDir = "reuters-opt-vectors";

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		HadoopUtil.delete(conf, new Path(outputDir));

		Path tokenizedPath = new Path(outputDir, DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);

		// use custom analyzer
		DocumentProcessor.tokenizeDocuments(new Path(inputDir), ReuterAnalyzer.class, tokenizedPath, conf);

		DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath,
				new Path(outputDir),
				DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER, conf,
				minSupport, maxNGramSize, minLLRValue, norm, true, reduceTasks,
				chunkSize, sequentialAccessOutput, false);

		Pair<Long[], List<Path>> dfData = TFIDFConverter.calculateDF(
				new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
				new Path(outputDir), conf, chunkSize);

		TFIDFConverter.processTfIdf(
				new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
				new Path(outputDir), conf, dfData, minDf, maxDFPercent, norm, true,
				sequentialAccessOutput, false, reduceTasks);

		String vectorsFolder = outputDir + "/tfidf-vectors";
		SequenceFile.Reader reader = new SequenceFile.Reader(
				fs,
				new Path(vectorsFolder, "part-r-00000"), conf);
		Text key = new Text();
		VectorWritable value = new VectorWritable();
		while (reader.next(key, value)) {
			System.out.println(key.toString() + " = > "
					+ value.get().asFormatString());
		}
		reader.close();
//		VectorDumper.main(new String[] { "--input", new Path(vectorsFolder, "part-r-00000").toString() });
//		SequenceFileDumper.main(new String[] { "--input", new Path(vectorsFolder, "part-r-00000").toString() });
	}
}
