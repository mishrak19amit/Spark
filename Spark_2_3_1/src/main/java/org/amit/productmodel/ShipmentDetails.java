package org.amit.productmodel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class ShipmentDetails implements Serializable{
	private static final long serialVersionUID = -3845211972116412730L;

	private Double length;

	private String lengthUnit;

	private Double breadth;

	private String breadthUnit;

	private Double height;

	private String heightUnit;

	private Double weight;

	private String weightUnit;

	private List<String> itemType;

	private String specialComments;

	private List<String> shippmentMot;
	
	public ShipmentDetails() {
		this.itemType = new ArrayList<String>();
		this.shippmentMot = new ArrayList<String>();
	}

	public Double getLength() {
		return length;
	}

	public void setLength(Double length) {
		this.length = length;
	}

	public String getLengthUnit() {
		return lengthUnit;
	}

	public void setLengthUnit(String lengthUnit) {
		this.lengthUnit = lengthUnit;
	}

	public Double getBreadth() {
		return breadth;
	}

	public void setBreadth(Double breadth) {
		this.breadth = breadth;
	}

	public String getBreadthUnit() {
		return breadthUnit;
	}

	public void setBreadthUnit(String breadthUnit) {
		this.breadthUnit = breadthUnit;
	}

	public Double getHeight() {
		return height;
	}

	public void setHeight(Double height) {
		this.height = height;
	}

	public String getHeightUnit() {
		return heightUnit;
	}

	public void setHeightUnit(String heightUnit) {
		this.heightUnit = heightUnit;
	}

	public Double getWeight() {
		return weight;
	}

	public void setWeight(Double weight) {
		this.weight = weight;
	}

	public String getWeightUnit() {
		return weightUnit;
	}

	public void setWeightUnit(String weightUnit) {
		this.weightUnit = weightUnit;
	}

	public List<String> getItemType() {
		return itemType;
	}

	public void setItemType(List<String> itemType) {
		this.itemType = itemType;
	}

	public String getSpecialComments() {
		return specialComments;
	}

	public void setSpecialComments(String specialComments) {
		this.specialComments = specialComments;
	}
	

	public List<String> getShippmentMot() {
		return shippmentMot;
	}

	public void setShippmentMot(List<String> shippmentMot) {
		this.shippmentMot = shippmentMot;
	}
	
	public void addMOT(String mot){
		if(!this.shippmentMot.contains(mot.toLowerCase()))
			this.shippmentMot.add(mot.toLowerCase());
	}

	@Override
	public String toString() {
		return "ShipmentDetails [length=" + length + ", lengthUnit="
				+ lengthUnit + ", breadth=" + breadth + ", breadthUnit="
				+ breadthUnit + ", height=" + height + ", heightUnit="
				+ heightUnit + ", weight=" + weight + ", weightUnit="
				+ weightUnit + ", itemType=" + itemType + ", specialComments="
				+ specialComments + ", shippmentMot=" + shippmentMot + "]";
	}	
}
