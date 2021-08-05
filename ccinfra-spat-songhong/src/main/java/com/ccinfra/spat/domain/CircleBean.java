package com.ccinfra.spat.domain;

public class CircleBean {

	private Center center;
	private double radius;
	
	public CircleBean() {
		super();
	}

	public CircleBean(Center center, double radius) {
		super();
		this.center = center;
		this.radius = radius;
	}

	public Center getCenter() {
		return center;
	}

	public void setCenter(Center center) {
		this.center = center;
	}

	public double getRadius() {
		return radius;
	}

	public void setRadius(double radius) {
		this.radius = radius;
	}
	
}
