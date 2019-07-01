package org.amit.productmodel;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Images {

	private String moglixImageNumber;
	
	private String uploadFileName;
	
	private int width;
	
	private int height;
	
	private int fileSize;
	
	private String sizeInfo;
	
	private String entityType;

    private Map<String, String> link;

    private Map<Date, String> oldUrls = new HashMap<Date, String>();

    private boolean deletedFlag;
	
    private String downloadedFrom;

	private Date createdOn;
	
	private Date updatedOn;

    public Images() {
        this.link = new HashMap<String, String>();
        this.oldUrls = new HashMap<Date, String>();
        this.createdOn = new Date();
        this.updatedOn = new Date();
        this.deletedFlag = false;
    }

	public String getMoglixImageNumber() {
		return moglixImageNumber;
	}

	public void setMoglixImageNumber(String moglixImageNumber) {
		this.moglixImageNumber = moglixImageNumber;
	}

	public String getUploadFileName() {
		return uploadFileName;
	}

	public void setUploadFileName(String uploadFileName) {
		this.uploadFileName = uploadFileName;
	}

	public int getWidth() {
		return width;
	}

	public void setWidth(int width) {
		this.width = width;
	}

	public int getHeight() {
		return height;
	}

	public void setHeight(int height) {
		this.height = height;
	}

	public int getFileSize() {
		return fileSize;
	}

	public void setFileSize(int fileSize) {
		this.fileSize = fileSize;
	}

	public String getSizeInfo() {
		return sizeInfo;
	}

	public void setSizeInfo(String sizeInfo) {
		this.sizeInfo = sizeInfo;
	}

	public String getEntityType() {
		return entityType;
	}

	public void setEntityType(String entityType) {
		this.entityType = entityType;
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

    public String getDownloadedFrom() {
        return downloadedFrom;
    }

    public void setDownloadedFrom(String downloadedFrom) {
        this.downloadedFrom = downloadedFrom;
    }

    public Map<String, String> getLink() {
        return link;
    }

    public void setLink(Map<String, String> link) {
        this.link = link;
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

    @Override
    public String toString() {
        return "Images [moglixImageNumber=" + moglixImageNumber + ", uploadFileName=" + uploadFileName + ", width=" + width + ", height=" + height
            + ", fileSize=" + fileSize + ", sizeInfo=" + sizeInfo + ", entityType=" + entityType + ", link=" + link + ", downloadedFrom=" + downloadedFrom
            + ", createdOn=" + createdOn + ", updatedOn=" + updatedOn + "]";
    }
}
