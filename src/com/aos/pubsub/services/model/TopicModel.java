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
	private static final long serialVersionUID = 1L;
	private String topicName;
	List<Message> messageList;
	long createdOn;
	boolean durable;
	long updatedOn;
	Set<String> subscriberList;
	
	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public List<Message> getMessageList() {
		return messageList;
	}

	public void setMessageList(List<Message> messageList) {
		this.messageList = messageList;
	}

	public long getCreatedOn() {
		return createdOn;
	}

	public void setCreatedOn(long createdOn) {
		this.createdOn = createdOn;
	}

	public boolean isDurable() {
		return durable;
	}

	public void setDurable(boolean durable) {
		this.durable = durable;
	}

	public long getUpdatedOn() {
		return updatedOn;
	}

	public void setUpdatedOn(long updatedOn) {
		this.updatedOn = updatedOn;
	}

	
}
