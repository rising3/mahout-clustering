package jp.opensquare.mahout.clustering.model;

public class BindFile {
	String title;
	Double distance;
	
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public Double getDistance() {
		return distance;
	}
	public void setDistance(Double distance) {
		this.distance = distance;
	}
	
	public BindFile(String title, Double distance) {
		super();
		this.title = title;
		this.distance = distance;
	}
}
