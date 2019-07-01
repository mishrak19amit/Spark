package org.amit.dataSet;

import java.util.List;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

@UDT(name="seo_details")
public class SEODetails {
	
	@Field(name="meta_description")
	private String metaDescription;
	@Field(name="title")
	private String title;
	@Field(name="meta_keywords")
	private List<String> metaKeywords;
	@Field(name="tags")
	private List<String> tags;
	
	
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	
	public String getMetaDescription() {
		return metaDescription;
	}
	public void setMetaDescription(String metaDescription) {
		this.metaDescription = metaDescription;
	}
	public List<String> getMetaKeywords() {
		return metaKeywords;
	}
	public void setMetaKeywords(List<String> metaKeywords) {
		this.metaKeywords = metaKeywords;
	}
	public List<String> getTags() {
		return tags;
	}
	public void setTags(List<String> tags) {
		this.tags = tags;
	}

    @Override
    public String toString() {
        return "SEODetails [metaDescription=" + metaDescription + ", title=" + title + ", metaKeywords=" + metaKeywords + ", tags=" + tags + "]";
    }
	
	
}