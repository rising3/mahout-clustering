package jp.opensquare.mahout.clustering.sample;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

public class ReuterAnalyzer extends Analyzer {
	private final Pattern alphabets = Pattern.compile("[a-z]+");

	@Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		TokenStream result = new StandardTokenizer(Version.LUCENE_30, reader);
		result = new StandardFilter(Version.LUCENE_30, result);
		result = new LowerCaseFilter(Version.LUCENE_30, result);
		// 's
		result = new EnglishPossessiveFilter(Version.LUCENE_30, result);
		// s, es, ing, ese, cation
		result = new KStemFilter(result);
		result = new StopFilter(Version.LUCENE_30, result, StandardAnalyzer.STOP_WORDS_SET);
				
		CharTermAttribute termAtt = (CharTermAttribute) result.addAttribute(CharTermAttribute.class);
		StringBuilder buf = new StringBuilder();
		try {
			while (result.incrementToken()) {
				if (termAtt.length() < 3)
					continue;
				String word = new String(termAtt.buffer(), 0, termAtt.length());
				Matcher m = alphabets.matcher(word);

				if (m.matches()) {
					buf.append(word).append(" ");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new WhitespaceTokenizer(Version.LUCENE_30, new StringReader(buf.toString()));
	}
}