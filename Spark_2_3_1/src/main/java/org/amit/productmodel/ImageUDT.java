package org.amit.productmodel;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ImageUDT implements Serializable{
	private static final long serialVersionUID = 4342188607026984696L;
	
    private Map<String, String> links;
	
	private String moglixImageNumber;
	
	private String altTag;
	
	private int position;
	
    public ImageUDT() {
        super();
        this.links = new HashMap<String, String>();
    }

    public Map<String, String> getLinks() {
        return links;
	}

    public void setLinks(Map<String, String> links) {
        this.links = links;
	}
	public String getAltTag() {
		return altTag;
	}
	public void setAltTag(String altTag) {
		this.altTag = altTag;
	}
	public String getMoglixImageNumber() {
		return moglixImageNumber;
	}
	public void setMoglixImageNumber(String moglixImageNumber) {
		this.moglixImageNumber = moglixImageNumber;
	}
	public int getPosition() {
		return position;
	}
	public void setPosition(int position) {
		this.position = position;
	}
	
    public void addLinks(Map<String, String> links) {
        for (String key : links.keySet())
            this.links.put(key, links.get(key));
    }

    @Override
    public String toString() {
        return "ImageUDT [links=" + links + ", moglixImageNumber=" + moglixImageNumber + ", altTag=" + altTag + ", position=" + position + "]";
    }

    /**
     * Maps <code>Image</code> to this object
     * 
     * @param image
     * @author Mohit Garg<mohit.garg@moglix.com>
     * @created_on 15-Oct-2015
     */
    public void mapFromImages(Images image) {
        this.moglixImageNumber = image.getMoglixImageNumber();
        this.links = image.getLink();
    }
}
