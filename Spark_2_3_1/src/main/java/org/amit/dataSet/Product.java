package org.amit.dataSet;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.FrozenValue;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;


@Table(keyspace = "products", name = "product_data")
public class Product {

	@PartitionKey
	@Column(name = "id_product")
	private String idProduct;

	@Column(name = "bulk_price_valid")
	private Boolean isBulkPriceValid;

	@Column(name = "category_code")
	private List<String> categoryCode;

	@Column(name = "product_name")
	private String productName;
	
	@Column(name = "product_type")
	private String productType;

	@Column(name = "description")
	private String description;
	
	@Column(name = "enterprise_type")
	private String entrerpriseType;

	@Column(name = "short_description")
	private String shortDescription;

	@Column(name = "brand_id")
	private UUID brandId;

	@Column(name = "manufacturer_id")
	private UUID manufacturerId;

	@Frozen
	@Column(name = "product_seo_details")
	private SEODetails productSeoDetails;

	@FrozenValue
	@Column(name = "images")
	private List<ImageUDT> images;
	
	@FrozenValue
	@Column(name = "datasheet")
	private List<DataSheetUDT> datasheets;

	/*@Column(name = "item_code")
	private String itemCode;
*/
	@Column(name = "key_feature")
	private List<String> keyFeature;

	@Column(name = "active")
	private boolean active;

	@FrozenValue
	@Column(name = "attribute_list")
	private Map<UUID, List<String>> attributeList;

	@Column(name = "shipment_details")
	private List<String> shipmentDetails;

	@Column(name = "countries_available_in")
	private List<String> countriesAvailableIn;

	@Column(name = "id_group")
	private String idGroup;

	@Column(name = "id_rule")
	private UUID ruleId;

	@Column(name = "group_name")
	private String groupName;

	@Column(name = "manually_grouped")
	private boolean manuallyGrouped;

	@Column(name = "grouped_flag")
	private boolean groupedFlag;

	@Column(name = "variant_flag")
	private boolean variant;

	@Column(name = "created_on")
	private Date createdOn;

	@Column(name = "updated_on")
	private Date updatedOn;

	@Column(name = "friendly_url")
	private String friendlyUrl;

	@Column(name = "link")
	private Map<String, String> link;
	
	@Column(name = "stored_link")
	private Map<String, String> storedLink;

	@Column(name = "old_urls")
	private Map<Date, String> oldUrls;

	@Column(name = "rating")
	private String rating;

	@Column(name = "verified_flag")
	private boolean verifiedFlag;

	@Column(name = "deleted_flag")
	private boolean deletedFlag;
	
	@Column(name = "items_in_pack")
	private String itemsInPack;

	@Column(name = "uom")
	private String uom;

	@Column(name = "quantity_uom")
	private Integer quantityUom;
	
	@Column(name = "internal_part_number")
	private String internalPartNumber;
	
	@Column(name = "mrp")
	private Map<String, Double> mrpByCountry;
	
	@Column(name = "available_for_order")
	private boolean availableForOrder;
	
	@Column(name = "last_updated_by")
	private String lastUpdatedBy;
	
	@Column(name = "group_verified")
	private boolean groupVerifiedFlag;
	
	@Column(name = "product_raw_name")
	private String productRawName;
	
	@Column(name="max_orderable_per_user_per_day")
	private Integer maxOrderablePerUserPerDay;
	
	@Frozen
	@Column(name = "shipping_details")
	private ShipmentDetails shippingDetails;
	
	@Column(name = "managed_inventory")
	private Boolean managedInventory;
	
	@Column(name = "quality_image")
	private Boolean qualityImage;
	
	@Column(name = "analytical_params")
	private Map<String, Double> analyticalParams;
	
	@Column(name = "excisable")
	private boolean excisable;
	
	@Column(name = "hsn")
	private String productHsn;
	
	@Column(name = "packaging")
	private String packaging;
	

	@Column(name = "group_details")
	private String groupDetails;
	

	@Column(name = "conversion_factor")
	private Double conversionFactor;
	
	@Column(name = "category_hierarchy")
	private Map<String,String> categoryHierarchy;
	
	@Column(name = "sub_group_id")
	private String subGroupID;

	@Column(name = "sub_group_details")
	private String subGroupDetails;

	public Product() {
		this.createdOn = new Date();
		this.updatedOn = new Date();
		this.variant = false;
		this.categoryCode = new ArrayList<String>();
		this.groupedFlag = false;
		this.manuallyGrouped = false;
		this.active = true;
		//this.sellingPriceRange = new HashMap<String, Map<String, Double>>();
		this.attributeList = new HashMap<UUID, List<String>>();
		this.deletedFlag = false;
		this.link = new HashMap<String, String>();
		this.oldUrls = new HashMap<Date, String>();
		this.storedLink = new HashMap<String, String>();
		this.availableForOrder = true;
		this.mrpByCountry = new HashMap<String, Double>();
		this.groupVerifiedFlag = false;
		this.managedInventory=false;
		this.qualityImage = false;
		this.analyticalParams = new HashMap<String, Double>();
		this.excisable = false;
        this.isBulkPriceValid = true;
        this.conversionFactor=0.0;
		this.quantityUom=0;
		this.categoryHierarchy = new LinkedHashMap<>();
	}


	public String getEntrerpriseType() {
		return entrerpriseType;
	}
	public void setEntrerpriseType(String entrerpriseType) {
		this.entrerpriseType = entrerpriseType;
	}
	
	public Boolean getIsBulkPriceValid() {
		return isBulkPriceValid;
	}

	public void setIsBulkPriceValid(Boolean isBulkPriceValid) {
		this.isBulkPriceValid = isBulkPriceValid;
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

	public boolean isGroupedFlag() {
		return groupedFlag;
	}

	public void setGroupedFlag(boolean groupedFlag) {
		this.groupedFlag = groupedFlag;
	}

	public String getIdProduct() {
		return idProduct;
	}

	public void setIdProduct(String idProduct) {
        this.idProduct = idProduct;
	}

	public List<String> getCategoryCode() {
		return categoryCode;
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

	public void setCategoryCode(List<String> categoryCode) {
		this.categoryCode = categoryCode;
	}

	/*
	 * public Map<Integer, HierarchyUDT> getCategoryHierarchy() { return
	 * categoryHierarchy; }
	 * 
	 * 
	 * 
	 * public void setCategoryHierarchy(Map<Integer, HierarchyUDT>
	 * categoryHierarchy) { this.categoryHierarchy = categoryHierarchy; }
	 */

	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
        this.productName = productName;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
        this.description = description;
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

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public void setImages(List<ImageUDT> images) {
		this.images = images;
	}

	/*public String getItemCode() {
		return itemCode;
	}

	public void setItemCode(String itemCode) {
		this.itemCode = itemCode;
	}
*/
	public List<String> getKeyFeature() {
		return keyFeature;
	}

	public void setKeyFeature(List<String> keyFeature) {
		//this.keyFeature = keyFeature;
        this.keyFeature = keyFeature;
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
		this.setStoredLinkMap(link);
	}

	public Map<Date, String> getOldUrls() {
		return oldUrls;
	}

	public void setOldUrls(Map<Date, String> oldUrls) {
		this.oldUrls = oldUrls;
	}

	public boolean isDeletedFlag() {
		return deletedFlag;
	}

	public void setDeletedFlag(boolean deletedFlag) {
		this.deletedFlag = deletedFlag;
	}
	
	public Map<String, String> getStoredLink() {
		return storedLink;
	}

	public void setStoredLink(Map<String, String> storedLink) {
		this.storedLink = storedLink;
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
	
	public String getItemsInPack() {
		return itemsInPack;
	}

	public void setItemsInPack(String itemsInPack) {
		this.itemsInPack = itemsInPack;
	}

	public String getUom(){
		return uom;
	}

	public void setUom(String uom){
		this.uom = uom;
	}

	public Integer getQuantityUom(){
		return quantityUom;
	}

	public void setQuantityUom(Integer quantityUom){
		this.quantityUom = quantityUom;
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

	public Boolean getQualityImage() {
		return qualityImage;
	}

	public void setQualityImage(Boolean qualityImage) {
		this.qualityImage = qualityImage;
	}

	public ShipmentDetails getShippingDetails() {
		return shippingDetails;
	}

	public void setShippingDetails(ShipmentDetails shippingDetails) {
		this.shippingDetails = shippingDetails;
	}	

	public Integer getMaxOrderablePerUserPerDay() {
		return maxOrderablePerUserPerDay;
	}

	public void setMaxOrderablePerUserPerDay(Integer maxOrderablePerUserPerDay) {
		this.maxOrderablePerUserPerDay = maxOrderablePerUserPerDay;
	}	

	public List<DataSheetUDT> getDatasheets() {
		return datasheets;
	}

	public void setDatasheets(List<DataSheetUDT> datasheets) {
		this.datasheets = datasheets;
	}

	public Boolean getManagedInventory() {
		return managedInventory;
	}

	public void setManagedInventory(Boolean managedInventory) {
		this.managedInventory = managedInventory;
	}
	
	public String getProductType() {
		return productType;
	}

	public void setProductType(String productType) {
		this.productType = productType;
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
	

	public Double getConversionFactor() {
		return conversionFactor;
	}

	public void setConversionFactor(Double conversionFactor) {
		this.conversionFactor = conversionFactor;
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

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

	/**
	 * adds category to list of parent categories
	 * 
	 * @param categoryCode
	 * @author Mohit Garg<mohit.garg@moglix.com>
	 * @created_on 05-Oct-2015
	 */
	public void addCategory(String categoryCode) {
		this.categoryCode.add(categoryCode);
	}
	
	
	
	
	

	/**
	 * adds new category hierarchy to least priority
	 * 
	 * @param categoryHierarchy
	 * @author Mohit Garg<mohit.garg@moglix.com>
	 * @throws MoglixCoreException
	 * @created_on 05-Oct-2015
	 */
	/*
	 * public void addCategoryHierarchy(List<String> categoryHierarchy) {
	 * Set<Integer> keys = this.categoryHierarchy.keySet(); int tmp = 10; for
	 * (int key : keys) { if (key > tmp) tmp = key + 10; } try {
	 * this.addCategoryHierarchy(categoryHierarchy, tmp); } catch
	 * (MoglixCoreException e) { // Nothing to do here as no exception will be
	 * thrown because the // priority is already checked. e.printStackTrace(); }
	 * }
	 */

	/**
	 * adds category hierarchy to given priority
	 * 
	 * @param categoryHierarchy
	 * @param priorty
	 * @throws MoglixCoreException
	 *             (if priority already present)
	 * @author Mohit Garg<mohit.garg@moglix.com>
	 * @created_on 05-Oct-2015
	 */
	/*
	 * public void addCategoryHierarchy(List<String> categoryHierarchy, int
	 * priorty) throws MoglixCoreException { if
	 * (this.categoryHierarchy.containsKey(priorty)) throw new
	 * MoglixCoreException(MoglixCoreError.
	 * SAME_PRIORITY_CATEGORY_HIERARCHY_ALREADY_PRESENT); HierarchyUDT hierarchy
	 * = new HierarchyUDT(); hierarchy.setHierarchyList(categoryHierarchy);
	 * this.categoryHierarchy.put(priorty, hierarchy); }
	 */

	/**
	 * returns highest priority hierarchy
	 * 
	 * @return
	 * @author Mohit Garg<mohit.garg@moglix.com>
	 * @created_on 05-Oct-2015
	 */
	/*
	 * public List<String> getParentCategoryHierarchy() { int tmp = 1000000;
	 * if(this.categoryHierarchy.keySet() != null &&
	 * !this.categoryHierarchy.isEmpty()){ Set<Integer> keys =
	 * this.categoryHierarchy.keySet(); for (Integer key : keys) { if (key <
	 * tmp) tmp = key; } try { return this.getParentCategoryHierarchy(tmp); }
	 * catch (MoglixCoreException e) { // Nothing to do as already checked
	 * e.printStackTrace(); } } return null;
	 * 
	 * }
	 */
	/**
	 * returns requested priority hierarchy
	 * 
	 * @param priority
	 * @return
	 * @throws MoglixCoreException
	 *             (if requested priority is not present)
	 * @author Mohit Garg<mohit.garg@moglix.com>
	 * @created_on 05-Oct-2015
	 */
	/*
	 * public List<String> getParentCategoryHierarchy(int priority) throws
	 * MoglixCoreException { if (!this.categoryHierarchy.containsKey(priority))
	 * throw new MoglixCoreException(MoglixCoreError.PRIORITY_NOT_PRESENT);
	 * return this.categoryHierarchy.get(priority); }
	 */

	public String getGroupDetails() {
		return groupDetails;
	}


	public void setGroupDetails(String groupDetails) {
		this.groupDetails = groupDetails;
	}


    /**
     * Function to update old url map
     * 
     * @param url
     * @author Mohit Garg<mohit.garg@moglix.com>
     * @created_on 22-Dec-2015
     */
    public void addOldUrl(String url) {
        if (this.oldUrls == null)
            this.oldUrls = new HashMap<Date, String>();

        this.oldUrls.put(new Date(), url.toLowerCase());
    }
    
    private void setStoredLinkMap(Map<String, String> link) {
		if(link != null){
			for(String key : link.keySet())
				storedLink.put(key, link.get(key).toLowerCase());
		}
	}
    
    public void addDataSheet(DataSheetUDT datasheet){
    	this.datasheets.add(datasheet);
    }
    
    public boolean putDataSheetAtPosition(DataSheetUDT datasheet, int position){
    	if(position>0){
	    	if(this.datasheets.size()<position)
	    		while(this.datasheets.size()<position){
	    			this.datasheets.add(new DataSheetUDT());
	    		}
	    	this.datasheets.add(position-1, datasheet);
	    	return true;
    	}else
    		return false;
    }


	public Map<String, String> getCategoryHierarchy() {
		return categoryHierarchy;
	}


	public void setCategoryHierarchy(Map<String, String> categoryHierarchy) {
		this.categoryHierarchy = categoryHierarchy;
	}
    
    
   
}
