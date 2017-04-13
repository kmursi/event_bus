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
	public String getIP() {
		return IP;
	}
	public void setIP(String IP) {
		this.IP = IP;
	}

	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
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
