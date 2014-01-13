package jp.opensquare.mahout.clustering.model;

import java.util.ArrayList;
import java.util.List;

public class PointCluster {
	Integer cluster;
	Long n;
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
	public Long getN() {
		return n;
	}
	public void setN(Long n) {
		this.n = n;
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

	public PointCluster(Integer cluster, Long n, Double cx, Double cy, Double rx, Double ry) {
		super();
		this.cluster = cluster;
		this.n = n;
		this.cx = cx;
		this.cy = cy;
		this.rx = rx;
		this.ry = ry;
	}
}
