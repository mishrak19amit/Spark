package org.amit.productmodel;

import java.util.Date;

public class DataSheetUDT {

	private String name;

	private String description;

	private Integer size;

	private Integer pages;

	private Date updatedOn;

	private String downloadedFrom;

	private String link;

	private String fileNumber;

	private String md5;

	private String fileType;

	public DataSheetUDT() {
		super();
		updatedOn = new Date();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Integer getSize() {
		return size;
	}

	public void setSize(Integer size) {
		this.size = size;
	}

	public Integer getPages() {
		return pages;
	}

	public void setPages(Integer pages) {
		this.pages = pages;
	}

	public Date getUpdatedOn() {
		return updatedOn;
	}

	public void setUpdatedOn(Date updatedOn) {
		this.updatedOn = updatedOn;
	}

	public String getDownloadedFrom() {
		return downloadedFrom;
	}

	public void setDownloadedFrom(String downloadedFrom) {
		this.downloadedFrom = downloadedFrom;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	public String getFileNumber() {
		return fileNumber;
	}

	public void setFileNumber(String fileNumber) {
		this.fileNumber = fileNumber;
	}

	public String getMd5() {
		return md5;
	}

	public void setMd5(String md5) {
		this.md5 = md5;
	}

	public String getFileType() {
		return fileType;
	}

	public void setFileType(String fileType) {
		this.fileType = fileType;
	}

	@Override
	public String toString() {
		return "DataSheetUDT [name=" + name + ", description=" + description + ", size=" + size + ", pages=" + pages
				+ ", updatedOn=" + updatedOn + ", downloadedFrom=" + downloadedFrom + ", link=" + link + ", fileNumber="
				+ fileNumber + ", md5=" + md5 + ", fileType=" + fileType + "]";
	}

}
