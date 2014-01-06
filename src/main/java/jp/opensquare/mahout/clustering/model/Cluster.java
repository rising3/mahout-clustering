package jp.opensquare.mahout.clustering.model;

import java.util.ArrayList;
import java.util.List;

public class Cluster {
	Integer cluster;
	Double cx;
	Double cy;
	Double rx;
	Double ry;
	List<Point> points = new ArrayList<Point>();

	public Integer getCluster() {
		return cluster;
	}
	public void setCluster(Integer cluster) {
		this.cluster = cluster;
	}
	public Double getCx() {
		return cx;
	}
	public void setCx(Double cx) {
		this.cx = cx;
	}
	public Double getCy() {
		return cy;
	}
	public void setCy(Double cy) {
		this.cy = cy;
	}
	public Double getRx() {
		return rx;
	}
	public void setRx(Double rx) {
		this.rx = rx;
	}
	public Double getRy() {
		return ry;
	}
	public void setRy(Double ry) {
		this.ry = ry;
	}
	public List<Point> getPoints() {
		return points;
	}

	public Cluster(Integer cluster, Double cx, Double cy, Double rx, Double ry) {
		super();
		this.cluster = cluster;
		this.cx = cx;
		this.cy = cy;
		this.rx = rx;
		this.ry = ry;
	}
}
