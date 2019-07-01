package org.amit.dataSet;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "products", name = "images")
public class Images {

	@PartitionKey
	@Column(name = "moglix_image_number")
	private String moglixImageNumber;
	
	@Column(name = "upload_file_name")
	private String uploadFileName;
	
	@Column(name = "width")
	private int width;
	
	@Column(name = "height")
	private int height;
	
	@Column(name = "file_size")
	private int fileSize;
	
	@Column(name = "size_info")
	private String sizeInfo;
	
	@Column(name = "entity_type")
	private String entityType;

    @Column(name = "link")
    private Map<String, String> link;

    @Column(name = "old_urls")
    private Map<Date, String> oldUrls = new HashMap<Date, String>();

    @Column(name = "deleted_flag")
    private boolean deletedFlag;
	
    @Column(name = "downloaded_from")
    private String downloadedFrom;

	@Column(name = "created_on")
	private Date createdOn;
	
	@Column(name = "updated_on")
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
