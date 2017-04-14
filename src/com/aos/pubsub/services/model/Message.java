package com.aos.pubsub.services.model;

import java.io.Serializable;

/**
 * Created by kmursi on 3/11/17.
 */
public class Message implements MessageMarker,Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int id;        //the seq number
    String data;      //holds message data or file name
    String topicName;
    long createdOn;
    boolean isDurable; //added for fault tolerance
    
    public boolean isDurable() {
		return isDurable;
	}
	public void setDurable(boolean isDurable) {
		this.isDurable = isDurable;
	}
	public Message(){
    	
    }
    public Message(int id, String data, String topicName)
    {
        this.id=id;
        this.data=data;
        this.topicName=topicName;
    }
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getData() {
		return data;
	}
	public void setData(String data) {
		this.data = data;
	}
	public String getTopicName() {
		return topicName;
	}
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
	public long getCreatedOn() {
		return createdOn;
	}

	public void setCreatedOn(long createdOn) {
		this.createdOn = createdOn;
	}
    
}