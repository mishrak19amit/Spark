package org.amit.pricingdatamigration;

import java.sql.Timestamp;
import java.util.Date;

public class ProductPricing {

private String id_product;
private String moglix_supplier_id;
private double retail_tp;
private double retail_moq;
private double quantity_available;
private String estimated_delivery;
private boolean out_of_stock_flag;
private boolean deleted_flag;
private Timestamp created_on;
private Timestamp updated_on;
//private Date valid_till = new Date(2025, 03, 31, 00, 00, 00);

public String getId_product() {
return id_product;
}

public void setId_product(String id_product) {
this.id_product = id_product;
}

public String getMoglix_supplier_id() {
return moglix_supplier_id;
}

public void setMoglix_supplier_id(String moglix_supplier_id) {
this.moglix_supplier_id = moglix_supplier_id;
}

public double getRetail_tp() {
return retail_tp;
}

public void setRetail_tp(double retail_tp) {
this.retail_tp = retail_tp;
}

public double getRetail_moq() {
return retail_moq;
}

public void setRetail_moq(double retail_moq) {
this.retail_moq = retail_moq;
}

public double getQuantity_available() {
return quantity_available;
}

public void setQuantity_available(double quantity_available) {
this.quantity_available = quantity_available;
}

public String getEstimated_delivery() {
return estimated_delivery;
}

public void setEstimated_delivery(String estimated_delivery) {
this.estimated_delivery = estimated_delivery;
}

public boolean isOut_of_stock_flag() {
return out_of_stock_flag;
}

public void setOut_of_stock_flag(boolean out_of_stock_flag) {
this.out_of_stock_flag = out_of_stock_flag;
}

public boolean isDeleted_flag() {
return deleted_flag;
}

public void setDeleted_flag(boolean deleted_flag) {
this.deleted_flag = deleted_flag;
}

public Timestamp getCreated_on() {
return created_on;
}

public void setCreated_on(Timestamp created_on) {
this.created_on = created_on;
}

public Timestamp getUpdated_on() {
return updated_on;
}

// public Date getValid_till() {
// return valid_till;
// }

}
