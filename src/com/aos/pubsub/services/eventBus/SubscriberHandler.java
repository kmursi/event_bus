package com.aos.pubsub.services.eventBus;

import com.aos.pubsub.services.model.Message;
import com.aos.pubsub.services.model.MessageMarker;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.net.Socket;
import java.util.*;


public class SubscriberHandler extends Thread {
    String IP, topicName;
    int port;
    ObjectOutputStream out;
    String subIP;
    Socket socket;
    int lastMessage;
    List<Message> subscriberMessage = new ArrayList();
    private ObjectMapper mapper = new ObjectMapper();
    /*********************************************************************************************/
    public SubscriberHandler(Socket socket, int port) {
        this.port = port;
        subIP = socket.getInetAddress().getHostName();
        this.socket = socket;
    }
    /*********************************************************************************************/
    public synchronized void run() {
        try {
            String receivedMessage, topicName;
            Message message;
            long time = new Date().getTime();
            {
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                out = new ObjectOutputStream(socket.getOutputStream());
                if (!(receivedMessage = (String) in.readObject()).equals(null)) { //received not empty
                    String splitter[] = receivedMessage.split("-");				  //split the string
                    topicName = splitter[0].trim();								  //split 0 = topic name
                    subscriberMessage = EventBusListener.indexBus.get(topicName); //get the topic array list
                    lastMessage = -1;
                    //System.out.println(lastMessage);
                    /////////////////////////////////////////////////////////////////////////
                    while (socket.isConnected()) {
                        subscriberMessage = EventBusListener.indexBus.get(topicName);//get the topic array list
                        if (lastMessage < subscriberMessage.size() && subscriberMessage.size() > 0) {
                            if (lastMessage == -1) {								//unknown index
                                lastMessage = getLastMessageIndex(time);			//get last message index
                            } else {
                            	System.out.println("=======================================================");
                            	System.out.println("Sending messages to "+subIP+"-"+port+" in topic '"+topicName+".");
                                for (int i = lastMessage; i < subscriberMessage.size(); i++) {
                                    message = subscriberMessage.get(i);				//store the message based on the search index
                                    //System.out.println("Connected to the subscriber..\n");
                                    out.flush();
                                    out.writeObject(mapper.writeValueAsString(message));//send the message
                                    out.flush();
                                }
                                System.out.println("New messages have been sent to "+subIP+"-"+port+".");
                                System.out.println("=======================================================\n");
                                lastMessage = subscriberMessage.size();
                            }
                        }
                        sleep(50000);
                    }
                    System.out.println("Subscriber " + subIP + ":" + port + " has been disconnected..!");
                }
            }
            socket.close();
            /////////////////////////////////////////////////////////////////////////
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
            try {
                socket.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /*********************************************************************************************/
    
    //This method handles the sending to the subscriber
    void pushToSubscriber(MessageMarker marker) {
        try {
            System.out.println("Connected to the subscriber..");
            out = new ObjectOutputStream(socket.getOutputStream());   							//initiate writer
            out.flush();
            out.writeObject(mapper.writeValueAsString(marker));                                 //send the message
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /*********************************************************************************************/

    synchronized public int getLastMessageIndex(long time) {
        int result=-1;
        try{
        long startTime=System.nanoTime();
        Message m = new Message();									//create temporary message to use it of comparison
        m.setCreatedOn(time);										//set the desired time
        result = Collections.binarySearch(subscriberMessage, m, new MessageComp());//do binary search
	        if (result < 0)
	            result = -1;
	        else
	        {
	        	long finishTime= System.nanoTime()-startTime;
	        	System.out.println("Binary search consumed:"+finishTime+" nsec to finish searching.");
	        	System.out.println("=======================================================\n");
	        }
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
        return result;

    }

}
/*********************************************************************************************/
class MessageComp implements Comparator<Message> {
    @Override
    public int compare(Message e1, Message e2) {
        if (e1.getCreatedOn() >= e2.getCreatedOn()) { //the specified date must be < the message date in order to be sent to the subscriber
        	//System.out.println("e1 "+e1.getCreatedOn()+" e2"+e2.getCreatedOn());
        	return 0;
        } else {
            return -1;
        }
    }
}
