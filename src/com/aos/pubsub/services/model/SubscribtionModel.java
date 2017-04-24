package com.aos.pubsub.services.model;

import java.io.Serializable;
import java.util.List;

/**
 * 
 * @author kmursi Holds the model for creating a topic
 */
public class SubscribtionModel implements Serializable,MessageMarker{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L; 
	private String topicName;											
	List<Message> messageList;
	long createdOn;
	boolean durable;
	long updatedOn;
	String IP;
	int port;
	public String getIP() {													//get IP address stored in the SubscribtionModel object
		return IP;
	}
	public void setIP(String IP) {											//set IP address to the SubscribtionModel object
		this.IP = IP;
	}

	public int getPort() {													//get port number stored in the SubscribtionModel object
		return port;
	}
	public void setPort(int port) {											//set port number to the SubscribtionModel object
		this.port = port;
	}
	public String getTopicName() {											//get topic name stored in the SubscribtionModel object
		return topicName;
	}

	public void setTopicName(String topicName) {							//set topic name to the SubscribtionModel object
		this.topicName = topicName;
	}

	public List<Message> getMessageList() {									//get message list stored in the SubscribtionModel object
		return messageList;
	}

	public void setMessageList(List<Message> messageList) {					//set message list to the SubscribtionModel object
		this.messageList = messageList;
	}

	public long getCreatedOn() {											//get created on stored in the SubscribtionModel object
		return createdOn;
	}

	public void setCreatedOn(long createdOn) {								//set created on to the SubscribtionModel object
		this.createdOn = createdOn;
	}

	public boolean isDurable() {											//get durability stored in the SubscribtionModel object
		return durable;
	}

	public void setDurable(boolean durable) {								//set durability to the SubscribtionModel object
		this.durable = durable;
	}

	public long getUpdatedOn() {											//get updated on stored in the SubscribtionModel object
		return updatedOn;	
	}

	public void setUpdatedOn(long updatedOn) {								//set updated on to the SubscribtionModel object
		this.updatedOn = updatedOn;
	}

	
}
