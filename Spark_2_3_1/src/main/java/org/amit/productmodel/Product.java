package org.amit.productmodel;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Product {

	private String idProduct;

	private Boolean isBulkPriceValid;

	private List<String> categoryCode;

	private String productName;

	private String productType;

	private String description;

	private String entrerpriseType;

	private String shortDescription;

	private UUID brandId;

	private UUID manufacturerId;

	private SEODetails productSeoDetails;

	private List<ImageUDT> images;

	private List<DataSheetUDT> datasheets;

	/*
	 * @Column(name = "item_code") private String itemCode;
	 */

	private List<String> keyFeature;

	private boolean active;

	private Map<UUID, List<String>> attributeList;

	private List<String> shipmentDetails;

	private List<String> countriesAvailableIn;

	private String idGroup;

	private UUID ruleId;

	private String groupName;

	private boolean manuallyGrouped;

	private boolean groupedFlag;

	private boolean variant;

	private Date createdOn;

	private Date updatedOn;

	private String friendlyUrl;

	private Map<String, String> link;

	private Map<String, String> storedLink;

	private Map<Date, String> oldUrls;

	private String rating;

	private boolean verifiedFlag;

	private boolean deletedFlag;

	private String itemsInPack;

	private String uom;

	private Integer quantityUom;

	private String internalPartNumber;

	private Map<String, Double> mrpByCountry;

	private boolean availableForOrder;

	private String lastUpdatedBy;

	private boolean groupVerifiedFlag;

	private String productRawName;

	private Integer maxOrderablePerUserPerDay;

	private ShipmentDetails shippingDetails;

	private Boolean managedInventory;

	private Boolean qualityImage;

	private Map<String, Double> analyticalParams;

	private boolean excisable;

	private String productHsn;

	private String packaging;

	private String groupDetails;

	private Double conversionFactor;

	private Map<String, String> categoryHierarchy;

	private String subGroupID;

	private String subGroupDetails;

	public String getIdProduct() {
		return idProduct;
	}

	public void setIdProduct(String idProduct) {
		this.idProduct = idProduct;
	}

	public Boolean getIsBulkPriceValid() {
		return isBulkPriceValid;
	}

	public void setIsBulkPriceValid(Boolean isBulkPriceValid) {
		this.isBulkPriceValid = isBulkPriceValid;
	}

	public List<String> getCategoryCode() {
		return categoryCode;
	}

	public void setCategoryCode(List<String> categoryCode) {
		this.categoryCode = categoryCode;
	}

	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
		this.productName = productName;
	}

	public String getProductType() {
		return productType;
	}

	public void setProductType(String productType) {
		this.productType = productType;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getEntrerpriseType() {
		return entrerpriseType;
	}

	public void setEntrerpriseType(String entrerpriseType) {
		this.entrerpriseType = entrerpriseType;
	}

	public String getShortDescription() {
		return shortDescription;
	}

	public void setShortDescription(String shortDescription) {
		this.shortDescription = shortDescription;
	}

	public UUID getBrandId() {
		return brandId;
	}

	public void setBrandId(UUID brandId) {
		this.brandId = brandId;
	}

	public UUID getManufacturerId() {
		return manufacturerId;
	}

	public void setManufacturerId(UUID manufacturerId) {
		this.manufacturerId = manufacturerId;
	}

	public SEODetails getProductSeoDetails() {
		return productSeoDetails;
	}

	public void setProductSeoDetails(SEODetails productSeoDetails) {
		this.productSeoDetails = productSeoDetails;
	}

	public List<ImageUDT> getImages() {
		return images;
	}

	public void setImages(List<ImageUDT> images) {
		this.images = images;
	}

	public List<DataSheetUDT> getDatasheets() {
		return datasheets;
	}

	public void setDatasheets(List<DataSheetUDT> datasheets) {
		this.datasheets = datasheets;
	}

	public List<String> getKeyFeature() {
		return keyFeature;
	}

	public void setKeyFeature(List<String> keyFeature) {
		this.keyFeature = keyFeature;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public Map<UUID, List<String>> getAttributeList() {
		return attributeList;
	}

	public void setAttributeList(Map<UUID, List<String>> attributeList) {
		this.attributeList = attributeList;
	}

	public List<String> getShipmentDetails() {
		return shipmentDetails;
	}

	public void setShipmentDetails(List<String> shipmentDetails) {
		this.shipmentDetails = shipmentDetails;
	}

	public List<String> getCountriesAvailableIn() {
		return countriesAvailableIn;
	}

	public void setCountriesAvailableIn(List<String> countriesAvailableIn) {
		this.countriesAvailableIn = countriesAvailableIn;
	}

	public String getIdGroup() {
		return idGroup;
	}

	public void setIdGroup(String idGroup) {
		this.idGroup = idGroup;
	}

	public UUID getRuleId() {
		return ruleId;
	}

	public void setRuleId(UUID ruleId) {
		this.ruleId = ruleId;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public boolean isManuallyGrouped() {
		return manuallyGrouped;
	}

	public void setManuallyGrouped(boolean manuallyGrouped) {
		this.manuallyGrouped = manuallyGrouped;
	}

	public boolean isGroupedFlag() {
		return groupedFlag;
	}

	public void setGroupedFlag(boolean groupedFlag) {
		this.groupedFlag = groupedFlag;
	}

	public boolean isVariant() {
		return variant;
	}

	public void setVariant(boolean variant) {
		this.variant = variant;
	}

	public Date getCreatedOn() {
		return createdOn;
	}

	public void setCreatedOn(Date createdOn) {
		this.createdOn = createdOn;
	}

	public Date getUpdatedOn() {
		return updatedOn;
	}

	public void setUpdatedOn(Date updatedOn) {
		this.updatedOn = updatedOn;
	}

	public String getFriendlyUrl() {
		return friendlyUrl;
	}

	public void setFriendlyUrl(String friendlyUrl) {
		this.friendlyUrl = friendlyUrl;
	}

	public Map<String, String> getLink() {
		return link;
	}

	public void setLink(Map<String, String> link) {
		this.link = link;
	}

	public Map<String, String> getStoredLink() {
		return storedLink;
	}

	public void setStoredLink(Map<String, String> storedLink) {
		this.storedLink = storedLink;
	}

	public Map<Date, String> getOldUrls() {
		return oldUrls;
	}

	public void setOldUrls(Map<Date, String> oldUrls) {
		this.oldUrls = oldUrls;
	}

	public String getRating() {
		return rating;
	}

	public void setRating(String rating) {
		this.rating = rating;
	}

	public boolean isVerifiedFlag() {
		return verifiedFlag;
	}

	public void setVerifiedFlag(boolean verifiedFlag) {
		this.verifiedFlag = verifiedFlag;
	}

	public boolean isDeletedFlag() {
		return deletedFlag;
	}

	public void setDeletedFlag(boolean deletedFlag) {
		this.deletedFlag = deletedFlag;
	}

	public String getItemsInPack() {
		return itemsInPack;
	}

	public void setItemsInPack(String itemsInPack) {
		this.itemsInPack = itemsInPack;
	}

	public String getUom() {
		return uom;
	}

	public void setUom(String uom) {
		this.uom = uom;
	}

	public Integer getQuantityUom() {
		return quantityUom;
	}

	public void setQuantityUom(Integer quantityUom) {
		this.quantityUom = quantityUom;
	}

	public String getInternalPartNumber() {
		return internalPartNumber;
	}

	public void setInternalPartNumber(String internalPartNumber) {
		this.internalPartNumber = internalPartNumber;
	}

	public Map<String, Double> getMrpByCountry() {
		return mrpByCountry;
	}

	public void setMrpByCountry(Map<String, Double> mrpByCountry) {
		this.mrpByCountry = mrpByCountry;
	}

	public boolean isAvailableForOrder() {
		return availableForOrder;
	}

	public void setAvailableForOrder(boolean availableForOrder) {
		this.availableForOrder = availableForOrder;
	}

	public String getLastUpdatedBy() {
		return lastUpdatedBy;
	}

	public void setLastUpdatedBy(String lastUpdatedBy) {
		this.lastUpdatedBy = lastUpdatedBy;
	}

	public boolean isGroupVerifiedFlag() {
		return groupVerifiedFlag;
	}

	public void setGroupVerifiedFlag(boolean groupVerifiedFlag) {
		this.groupVerifiedFlag = groupVerifiedFlag;
	}

	public String getProductRawName() {
		return productRawName;
	}

	public void setProductRawName(String productRawName) {
		this.productRawName = productRawName;
	}

	public Integer getMaxOrderablePerUserPerDay() {
		return maxOrderablePerUserPerDay;
	}

	public void setMaxOrderablePerUserPerDay(Integer maxOrderablePerUserPerDay) {
		this.maxOrderablePerUserPerDay = maxOrderablePerUserPerDay;
	}

	public ShipmentDetails getShippingDetails() {
		return shippingDetails;
	}

	public void setShippingDetails(ShipmentDetails shippingDetails) {
		this.shippingDetails = shippingDetails;
	}

	public Boolean getManagedInventory() {
		return managedInventory;
	}

	public void setManagedInventory(Boolean managedInventory) {
		this.managedInventory = managedInventory;
	}

	public Boolean getQualityImage() {
		return qualityImage;
	}

	public void setQualityImage(Boolean qualityImage) {
		this.qualityImage = qualityImage;
	}

	public Map<String, Double> getAnalyticalParams() {
		return analyticalParams;
	}

	public void setAnalyticalParams(Map<String, Double> analyticalParams) {
		this.analyticalParams = analyticalParams;
	}

	public boolean isExcisable() {
		return excisable;
	}

	public void setExcisable(boolean excisable) {
		this.excisable = excisable;
	}

	public String getProductHsn() {
		return productHsn;
	}

	public void setProductHsn(String productHsn) {
		this.productHsn = productHsn;
	}

	public String getPackaging() {
		return packaging;
	}

	public void setPackaging(String packaging) {
		this.packaging = packaging;
	}

	public String getGroupDetails() {
		return groupDetails;
	}

	public void setGroupDetails(String groupDetails) {
		this.groupDetails = groupDetails;
	}

	public Double getConversionFactor() {
		return conversionFactor;
	}

	public void setConversionFactor(Double conversionFactor) {
		this.conversionFactor = conversionFactor;
	}

	public Map<String, String> getCategoryHierarchy() {
		return categoryHierarchy;
	}

	public void setCategoryHierarchy(Map<String, String> categoryHierarchy) {
		this.categoryHierarchy = categoryHierarchy;
	}

	public String getSubGroupID() {
		return subGroupID;
	}

	public void setSubGroupID(String subGroupID) {
		this.subGroupID = subGroupID;
	}

	public String getSubGroupDetails() {
		return subGroupDetails;
	}

	public void setSubGroupDetails(String subGroupDetails) {
		this.subGroupDetails = subGroupDetails;
	}

	public Product() {
		this.createdOn = new Date();
		this.updatedOn = new Date();
		this.variant = false;
		this.categoryCode = new ArrayList<String>();
		this.groupedFlag = false;
		this.manuallyGrouped = false;
		this.active = true;
		// this.sellingPriceRange = new HashMap<String, Map<String, Double>>();
		this.attributeList = new HashMap<UUID, List<String>>();
		this.deletedFlag = false;
		this.link = new HashMap<String, String>();
		this.oldUrls = new HashMap<Date, String>();
		this.storedLink = new HashMap<String, String>();
		this.availableForOrder = true;
		this.mrpByCountry = new HashMap<String, Double>();
		this.groupVerifiedFlag = false;
		this.managedInventory = false;
		this.qualityImage = false;
		this.analyticalParams = new HashMap<String, Double>();
		this.excisable = false;
		this.isBulkPriceValid = true;
		this.conversionFactor = 0.0;
		this.quantityUom = 0;
		this.categoryHierarchy = new LinkedHashMap<>();
	}

}
