package com.aos.pubsub.services.model;

import java.io.Serializable;

/**
 * Created by kmursi on 3/11/17.
 */
public class Message implements MessageMarker,Serializable {
	private static final long serialVersionUID = 1L;
	int id;        										//message id (serial number)
    String data;      									//holds message data or file name
    String topicName;								    //message topic name
    long createdOn, expiration;							//date of creation and expiration
    boolean isDurable; 									//added for fault tolerance
    
    /*********************************************************************************************/
    
    public Message(){
    	
    }
    
    /*********************************************************************************************/
    
    public Message(int id, String data, String topicName)//receives id, data , and topic name 
    {
        this.id=id;
        this.data=data;
        this.topicName=topicName;
    }
    
    /*********************************************************************************************/
    
    public boolean isDurable() {
		return isDurable;								//return durability
	}
    
    /*********************************************************************************************/
    
	public void setDurable(boolean isDurable) {
		this.isDurable = isDurable;						//set durability
	}
	
	/*********************************************************************************************/
	
	public int getId() {								//get message id
		return id;
	}
	
	/*********************************************************************************************/
	
	public void setId(int id) {							//set message id, receives id
		this.id = id;
	}
	
	/*********************************************************************************************/
	
	public String getData() {							//get message content
		return data;
	}
	
	/*********************************************************************************************/
	
	public void setData(String data) {					//set information to the message
		this.data = data;
	}
	
	/*********************************************************************************************/
	
	public String getTopicName() {						//get message topic
		return topicName;
	}
	
	/*********************************************************************************************/
	
	public void setTopicName(String topicName) {		//set message topic
		this.topicName = topicName;
	}
	
	/*********************************************************************************************/
	
	public long getCreatedOn() {						//get created on 
		return createdOn;
	}
	
	/*********************************************************************************************/
	
	public void setCreatedOn(long createdOn) {			//set created on
		this.createdOn = createdOn;
	}
	
	/*********************************************************************************************/
	
	public long getExpirationDate() {					//get expiration
		return expiration;
	}
	
	/*********************************************************************************************/
	
	public void setExpirationDate(long expiration) {	//set expiration date
		this.expiration = expiration;
	}
	
}