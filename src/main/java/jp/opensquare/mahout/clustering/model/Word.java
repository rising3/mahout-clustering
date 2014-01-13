package jp.opensquare.mahout.clustering.model;

public class Word {
	Integer key;
	String name;
	Double value;

	public Integer getKey() {
		return key;
	}

	public void setKey(Integer key) {
		this.key = key;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
		this.value = value;
	}

	public Word(Integer key, String name, Double value) {
		super();
		this.key = key;
		this.name = name;
		this.value = value;
	}
	
	public Word() {
		super();
	}
}
