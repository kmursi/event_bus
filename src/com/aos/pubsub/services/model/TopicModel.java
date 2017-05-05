package com.aos.pubsub.services.model;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * 
 * @author Amit Holds the model for creating a topic
 */
public class TopicModel implements Serializable,MessageMarker{
	
	/**
	 * 
	 */

	private String topicName;
	List<Message> messageList;
	long createdOn;
	boolean durable;
	long updatedOn;
	Set<String> subscriberList;
	
	public String getTopicName() {				//get topic name of the Topic model object
		return topicName;
	}

	public void setTopicName(String topicName) {//set topic name to the Topic model object
		this.topicName = topicName;
	}

	public List<Message> getMessageList() {		//get message list of the topic
		return messageList;
	}

	public void setMessageList(List<Message> messageList) {	//set list of messages to the topic
		this.messageList = messageList;
	}

	public long getCreatedOn() {				//get topic creation time
		return createdOn;
	}

	public void setCreatedOn(long createdOn) {	//set topic creation time
		this.createdOn = createdOn;
	}

	public boolean isDurable() {				//get topic durability
		return durable;
	}

	public void setDurable(boolean durable) {	//set topic durability
		this.durable = durable;
	}

	public long getUpdatedOn() {				//get last update time
		return updatedOn;
	}

	public void setUpdatedOn(long updatedOn) {	//set update time
		this.updatedOn = updatedOn;
	}

	
}
