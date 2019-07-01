package org.amit.dataSet;

public class ProductCheck {

	private String id_product;
	private boolean active;

	public boolean isManuallyGrouped() {
		return manuallyGrouped;
	}

	public void setManuallyGrouped(boolean manuallyGrouped) {
		this.manuallyGrouped = manuallyGrouped;
	}

	private boolean manuallyGrouped;

	public String getId_product() {
		return id_product;
	}

	public void setId_product(String idproduct) {
		this.id_product = idproduct;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

}
