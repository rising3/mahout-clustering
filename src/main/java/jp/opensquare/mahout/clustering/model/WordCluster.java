package jp.opensquare.mahout.clustering.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class WordCluster {
	Integer cluster;
	Long n;

	Map <Double, Word> topTerms = new HashMap <Double, Word>();
	List<Word> texts = new ArrayList<Word>();
	List<BindFile> files = new ArrayList<BindFile>();

	public Integer getCluster() {
		return cluster;
	}
	public void setCluster(Integer cluster) {
		this.cluster = cluster;
	}
	public Long getN() {
		return n;
	}
	public void setN(Long n) {
		this.n = n;
	}

	public List<Word> getTopTerms() {
		return getTopTerms(10);
	}

	public List<BindFile> getFiles() {
		return this.files;
	}
	
	public List<Word> getTopTerms(int n) {
		List<Word> list = new ArrayList<Word>();
		SortedSet<Double> keys = new TreeSet<Double>(this.topTerms.keySet()).descendingSet();
		for (Double key : keys) { 
			list.add(this.topTerms.get(key));
			if(list.size() == n) {
				break;
			}
		}
		return list;
	}

	public void add(Word text) {
		this.texts.add(text);
		this.topTerms.put(text.getValue(), text);
	}
	
	public WordCluster(Integer cluster, Long n) {
		super();
		this.cluster = cluster;
		this.n = n;
	}

	public WordCluster() {
		super();
	}		
}
