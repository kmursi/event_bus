package com.aos.pubsub.services.eventBus;

import com.aos.pubsub.services.model.Message;
import com.aos.pubsub.services.model.MessageMarker;
import com.aos.pubsub.services.model.SubscribtionModel;
import com.aos.pubsub.services.model.TopicModel;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
//
/**
 * Created by kmursi on 3/10/17.
 */

public class EventBusListener extends Thread {
    static int maxsize = 0;
    static volatile  Map<String, List<Message>> indexBus = new ConcurrentHashMap<String, List<Message>>(); //Main eventbus
    static volatile Map<String, List<Message>> durableIndexBus = new ConcurrentHashMap<String, List<Message>>(); //durable message eventbus
    static volatile Map<String, Set<SubscribtionModel>> topicSubscibtionList = new ConcurrentHashMap<String, Set<SubscribtionModel>>(); //list of subscribers and their topics
    Socket conn;
    ObjectMapper mapper = new ObjectMapper();
    int listeningPort, publishTopicPort = 60000, publishMessagePort = 60001, SubscribtionRequest = 60002, subscriberPullRequest = 60003, messagesByTime = 60004;    //each port hold a deffirent function
    private ObjectOutputStream writer;

    /*********************************************************************************************/
    
    public EventBusListener(int port) {
    	this.listeningPort = port; 
    }
    
    /*********************************************************************************************/
    
    public EventBusListener(Socket s, int port) {
        conn = s;                                       			// let the local socket to have the value of the received one
        this.listeningPort = port;                     				// let the local port to have the value of the received one
    }
    
    /*********************************************************************************************/
    
    public static void prepareEventBus() throws IOException {
    	long startTime=System.currentTimeMillis();
        String topicObjectPath = Main.topicObjectPath;				//get log file location
        FileInputStream fileTopic;
        ObjectInputStream topicReader = null, messageReader = null;
        File folder = Main.parentFolder;
        /////////////////////////////////////////////////////////////////////////////
        if (Files.size(Paths.get(folder + topicObjectPath)) > 0) {	 //if the log is not empty
            fileTopic = new FileInputStream(folder + topicObjectPath);
            topicReader = new ObjectInputStream(fileTopic);
            /////////////////////////////////////////////////////////////////////////////
            try {
            	Map<String, List<Message>> topicObject=new ConcurrentHashMap<String, List<Message>>();
                while (true) {
                    topicObject = (ConcurrentHashMap<String, List<Message>>)topicReader.readObject();
                    indexBus = topicObject;							//load the main eventbus
                    durableIndexBus = topicObject;					//load the durable eventbus
                }
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {

            }
            /////////////////////////////////////////////////////////////////////////////
            try {
                System.out.println("*********************************************************************************************");
                System.out.println("Printing the reloaded event bus ..!");
                for (Map.Entry<String, List<Message>> entry : indexBus.entrySet()) {
                    System.out.println("\nTopic name " + entry.getKey() + " conatains below message:");
                    for (Message m : entry.getValue()) {			//print the current evenbus after reloading
                        System.out.println(m.getData());
                    }
                }
                long duration= System.currentTimeMillis()-startTime;
                System.out.println("The event bus consumed "+duration+" msec to reload !\n");
                System.out.println("*********************************************************************************************");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (messageReader != null)
                        messageReader.close();
                    if (topicReader != null)
                        topicReader.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                /////////////////////////////////////////////////////////////////////////////
            }
        } else {
            System.out.println("\nThis server has no previously recorded topics..!\n");
        }
    }
    
    /*********************************************************************************************/
    
    public static void prepareSubscriptionList() throws IOException {
    	long startTime= System.currentTimeMillis();
        String subscribersObjectPath = Main.subscriptionObjectPath; //get the subscribers log path
        FileInputStream subscriptionInput;
        ObjectInputStream subscriptionReader = null, messageReader = null;
        File folder = Main.parentFolder;
        /////////////////////////////////////////////////////////////////////////////
        if (Files.size(Paths.get(folder + subscribersObjectPath)) > 0) {
            subscriptionInput = new FileInputStream(folder + subscribersObjectPath);
            subscriptionReader = new ObjectInputStream(subscriptionInput);
            Map<String, Set<SubscribtionModel>> readObject = new ConcurrentHashMap<String, Set<SubscribtionModel>>();;
            /////////////////////////////////////////////////////////////////////////////
            try {
                while (true) {
                    readObject = (ConcurrentHashMap<String, Set<SubscribtionModel>>) subscriptionReader.readObject(); //read the file
                    topicSubscibtionList = readObject;					//store the log list into local list
                }
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
            }
            /////////////////////////////////////////////////////////////////////////////
            try {
                System.out.println("*********************************************************************************************");
                System.out.println("Printing recreated topic subscibtion list..!");
                for (Map.Entry<String, Set<SubscribtionModel>> entry : topicSubscibtionList.entrySet()) {
                    System.out.println("\nTopic name " + entry.getKey() + " conatains the following subscribers:");
                    for (SubscribtionModel m : entry.getValue()) {       //print the subscribers list
                        System.out.println("Subscriper IP:" + m.getIP() + ", Subscriber port: " + m.getPort() + "\n");
                    }
                }
                long duration= System.currentTimeMillis()-startTime;
                System.out.println("The event bus consumed "+duration+" msec to reload !\n");
                System.out.println("*********************************************************************************************");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (messageReader != null)
                        messageReader.close();
                    if (subscriptionReader != null)
                        subscriptionReader.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                /////////////////////////////////////////////////////////////////////////////
            }
        } else {
            System.out.println("\nThis server has no previously recorded subscribers..!\n");
        }

    }

    /*********************************************************************************************/

    public synchronized void run() {
        if (listeningPort == publishTopicPort)          		//call receivingTopicRequest() if its port is connected with the server
            receivingTopicRequest();
            /////////////////////////////////////////////////////////////////////////////
        else if (listeningPort == publishMessagePort)        	//call receivingMessage() if its port is connected the server
            receivingMessage();
        /////////////////////////////////////////////////////////////////////////////
        else if (listeningPort == SubscribtionRequest)        	//call SubscribeTopicRequest() if its port is connected the server
            SubscribeTopicRequest();
        /////////////////////////////////////////////////////////////////////////////
        else if (listeningPort == subscriberPullRequest)        //call subscriberPullRequest() if its port is connected the server
            subscriberPullRequest();
        /////////////////////////////////////////////////////////////////////////////
        else if (listeningPort == messagesByTime)        		//call getMessagesBasedOnTime() if its port is connected the server
            getMessagesBasedOnTime();
        else if (listeningPort == 0)        		//call getMessagesBasedOnTime() if its port is connected the server
        	grbageCollector();
        
    }

    /*********************************************************************************************/
    public void grbageCollector() {
        while (true) {
            Message m;
            long currentTime = new Date().getTime();									//store the current time
            for (Entry<String, List<Message>> entry : EventBusListener.indexBus.entrySet()) {
                String key = entry.getKey();											//save the message key
                List<Message> list = entry.getValue();									//save the message value
                /////////////////////////////////////////////////////////////////////////////
                for (int i = 0; i < list.size(); i++) {
                    m = list.get(i);													//get the message
                    if (currentTime > m.getExpirationDate()) {							//if the message expired
                        System.out.println("Message " + EventBusListener.indexBus.get(key).get(i).getId() + " !has been deleted from the main EventBus due to its expiration date !\n");
                        EventBusListener.indexBus.get(key).remove(i);					//remove the message from the event bus
                    }
                }
                ///////////////////////////////////////////////////////////////////////////// do the same for the durable eventbus
                for (Entry<String, List<Message>> e : EventBusListener.durableIndexBus.entrySet()) {
                    String key2 = e.getKey();
                    List<Message> durableList = e.getValue();
                    for (int i = 0; i < durableList.size(); i++) {
                        m = durableList.get(i);
                        if (currentTime > m.getExpirationDate()) {
                            System.out.println("Message " + EventBusListener.durableIndexBus.get(key2).get(i).getId() + " has been deleted from the durable messages queue due to its expiration date !\n");
                            EventBusListener.durableIndexBus.get(key2).remove(i);
                        }
                    }
                    
                }
            }
            topicLog();
            System.out.println("Garbage collector finished cleaning the EvenetBus !\n");
            try {
                sleep(50000);
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            
        }
    }
    
    synchronized void receivingTopicRequest() {
        try {
            String topicName;                             		// define an integer peer ID which is the  peer port
            List<Message> messageList;                    		//String array used for splitting the received message
            /////////////////////////////////////////////////////////////////////////////
            String pubIP = conn.getInetAddress().getHostName(); //save the peer IP into peerIP
            ObjectInputStream in = new ObjectInputStream(conn.getInputStream()); //initiate object input stream to read from peer
            MessageMarker messageMarker;
            String recievedString = null;
            /////////////////////////////////////////////////////////////////////////////
            try {
                recievedString = (String) in.readObject();               //read
                messageMarker = mapper.readValue(recievedString, TopicModel.class);
            } catch (JsonMappingException | JsonParseException jEx) {
                messageMarker = mapper.readValue(recievedString, Message.class);
            }
            /////////////////////////////////////////////////////////////////////////////
            TopicModel topic = null;
            Message messageModel = null;
            if (messageMarker instanceof TopicModel) {			//make sure the received message is a topic model
                topic = (TopicModel) messageMarker;
                long createdDate = new Date().getTime();
                topic.setCreatedOn(createdDate);
                topic.setUpdatedOn(createdDate);
                messageList = topic.getMessageList();
                topicName = topic.getTopicName();
                if (messageList == null) {
                    messageList = new ArrayList<Message>();		//initiate new list if the current list is null
                    topic.setMessageList(messageList);
                }
                System.out.println("Topic " + topicName + "  created in the event bus \n");
                /////////////////////////////////////////////////////////////////////////////
                indexBus.put(topicName, messageList);             //store the hashmap element
                if (topic.isDurable())                            // log only when topic is durable
                {
                    durableIndexBus.put(topicName, messageList);
                }

            } else {
                System.out.println("Invalid object passed . returning....");

            }
            /////////////////////////////////////////////////////////////////////////////
            in.close();                                         //close reader
            conn.close();                                       //close connection
        }
        /////////////////////////////////////////////////////////////////////////////
        catch (UnknownHostException unknownHost) {              //To Handle Unknown Host Exception
            System.err.println("host not available..!");
        } catch (IOException ioException) {                      //To Handle Input-Output Exception
            ioException.printStackTrace();
        } catch (Exception e) {                                  //track general errors
            e.printStackTrace();
            System.out.println(e.toString());
        } finally {
            System.out.println("Type the action number as following:");
            System.out.println("1. To exit.\n");
            Thread.currentThread().stop();
        }
    }

    /*********************************************************************************************/

    synchronized void SubscribeTopicRequest() {
        try {
            String topicName, reply = null;                      // define an integer peer ID which is the  peer port
            int peerID;
            /////////////////////////////////////////////////////////////////////////////
            String subIP = conn.getInetAddress().getHostName();  //save the peer IP into peerIP
            ObjectInputStream in = new ObjectInputStream(conn.getInputStream()); //initiate object input stream to read from peer
            String recievedString = null;
            recievedString = (String) in.readObject();
            if (!recievedString.equals(null)) {
                String messageArray[] = recievedString.split("-");//split by -
                peerID = Integer.parseInt(messageArray[0]);
                topicName = messageArray[1];					 //store topic name
                if (indexBus.containsKey(topicName)) {
                    ////////////////////////////////////////////////////////////
                    SubscribtionModel subModel = new SubscribtionModel();
                    subModel.setPort(peerID);					//set port number
                    subModel.setIP(subIP);						//set IP address
                    subModel.setTopicName(topicName);			//set topic name
                    Set<SubscribtionModel> list = topicSubscibtionList.get(subModel.getTopicName());
                    if (list == null) {
                        list = new HashSet<>();
                    }
                    list.add(subModel);
                    //////////////////////////////////////////////////////////////
                    if (topicSubscibtionList.containsKey(topicName)) {	//search for the topic name
                        reply = "You are already subcribed to " + topicName + " !\n";
                    } else {
                        topicSubscibtionList.put(subModel.getTopicName(), list);
                        Subscription_Recorder();						//record the subscriber to a log file
                        reply = "You are now subcribing topic '" + topicName + "'"; 
                        System.out.println("Subscribtion request from " + subIP + ":" + peerID + " accepted for topic " + topicName + "\n");
                    }
                } else {
                    System.out.println("No message Received!\n");
                    reply = "Topic not found\n";
                }
            } else {
                System.out.println("No message Received!\n");
                reply = "Topic not found\n";
            }
            ObjectOutputStream out = new ObjectOutputStream(conn.getOutputStream()); //define object writer
            out.writeObject(reply);                                                  //write the reply to the peer
            out.flush();
            in.close();
            out.close();
            conn.close();
        } catch (Exception e) {

        }
    }
    
    /*********************************************************************************************/
    
    public void subscriberPullRequest() {
        new SubscriberHandler(conn, listeningPort).start(); //new thread to handle the subscriber
    }
    
    /*********************************************************************************************
     * this method used to record the event bus into the log file
     ********************************************************************************************* */
    
    synchronized public void topicLog() {
        FileOutputStream file = null;
        File folder = Main.parentFolder;
        //////////////////////////////////////////////////////////////
        try {
            file = new FileOutputStream(folder + Main.topicObjectPath);
            writer = new ObjectOutputStream(file);			//open a stream with the log file
            Map<String, List<Message>> tempDurableIndexBus= new ConcurrentHashMap<String, List<Message>>();
            tempDurableIndexBus=durableIndexBus;
            writer.writeObject(tempDurableIndexBus);			//write the durable eventbus into the log file
            writer.close();
            file.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            try {
                writer.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
    /*********************************************************************************************/
    
    public void Subscription_Recorder() 
    {
        FileOutputStream file = null;
        File folder = Main.parentFolder;
        try {
            file = new FileOutputStream(folder + Main.subscriptionObjectPath);
            writer = new ObjectOutputStream(file);
            writer.writeObject(topicSubscibtionList);		//write the subscription eventbus into the log file
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
    /*********************************************************************************************/
    
    synchronized void receivingMessage() {
        try {
            String topicName;                             
            List<Message> messageList;                    
            /////////////////////////////////////////////////////////////////////////////
            String pubIP = conn.getInetAddress().getHostName();    //save the peer IP into pubIP
            ObjectInputStream in = null;
            //////////////////////////////////////////////////////////////
            while (conn.getInputStream().available() != -1) {
                in = new ObjectInputStream(conn.getInputStream()); //initiate object input stream to read from peer
                MessageMarker messageMarker;
                String recievedString = null;
                try {
                    recievedString = (String) in.readObject();    //read
                    messageMarker = mapper.readValue(recievedString, TopicModel.class);
                } catch (JsonMappingException | JsonParseException jEx) {
                    messageMarker = mapper.readValue(recievedString, Message.class); //JSON parser
                }
                Message messageModel = null;
                //////////////////////////////////////////////////////////////
                if (messageMarker instanceof Message) {				//make sure the message is a type of Message object
                    messageModel = (Message) messageMarker;
                    String topicNameStr = messageModel.getTopicName();
                    messageList = indexBus.get(topicNameStr);		//get the array list of the specified topic name
                    Message m = new Message(messageList.size(), messageModel.getData(), topicNameStr);
                    m.setDurable(messageModel.isDurable());			//set the message durability
                    m.setExpirationDate(messageModel.getExpirationDate());//set expiration date based on the publisher date	
                    m.setCreatedOn(messageModel.getCreatedOn());
                    //////////////////////////////////////////////////////////////
                    if (messageList != null) {
                        messageList.add(m);
                    }
                    //////////////////////////////////////////////////////////////
                    indexBus.put(topicNameStr, messageList);		//add the message to the main eventbus
                    if (m.isDurable())  							//only durable messaged gets persisted
                    {
                        durableIndexBus.put(topicNameStr, messageList);
                    }
                    System.out.println("Added new message  " + messageModel.getData() + " in topic " + topicNameStr);
                } else {
                    System.out.println("Invalid object passed . returning....");
                }
                /////////////////////////////////////////////////////////////////////////////
            }
            in.close();                                         	//close reader
            conn.close();                                       	//close connection
        }
        /////////////////////////////////////////////////////////////////////////////
        catch (EOFException eof) {
            System.out.println("finished publishing topics");
        } catch (UnknownHostException unknownHost) {                 //To Handle Unknown Host Exception
            System.err.println("host not available..!");
        } catch (IOException ioException) {                          //To Handle Input-Output Exception
            ioException.printStackTrace();
        } catch (Exception e) {                                      //track general errors
            e.printStackTrace();
            System.out.println(e.toString());
        } finally {
            System.out.println("\nType the action number as following:");
            System.out.println("1. To exit.\n");
            Thread.currentThread().stop();
        }
    }
    
    /*********************************************************************************************/
    
    public synchronized void getMessagesBasedOnTime() {
        try {
            String receivedMessage, topicName;
            Message message;
            ObjectOutputStream out;
            String subIP = conn.getInetAddress().getHostName();
            Socket socket;
            Long lastMessageDate;
            List<Message> subscriberMessage = null;
            //////////////////////////////////////////////////////////////
            {
                ObjectInputStream in = new ObjectInputStream(conn.getInputStream());
                out = new ObjectOutputStream(conn.getOutputStream());
                if (!(receivedMessage = (String) in.readObject()).equals(null)) {
                    String splitter[] = receivedMessage.split("-");			//split the received message by -
                    topicName = splitter[0].trim();							// split 0 = topic name
                    lastMessageDate = Long.parseLong(splitter[1].trim());	//split 1 = the desired date
                    /////////////////////////////////////////////////////////////////////////
                    //while(true)
                    {
                    if (conn.isConnected()) {
                        if (indexBus.containsKey(topicName)) {
                            subscriberMessage = EventBusListener.indexBus.get(topicName); //get the required topic array list
                            long startTime=System.nanoTime();
                            System.out.println("Sequential serch started in the eventbus\n");
                            boolean flag=false;
                            for (int i = 0; i < subscriberMessage.size(); i++) {
                                message = subscriberMessage.get(i);
                                if (message.getCreatedOn() > lastMessageDate) {			//check if the message date > the required date
                                    out.flush();
                                    out.writeObject(mapper.writeValueAsString(message)); //send the message
                                    out.flush();
                                    if(flag==false)
                                    {
                                    	flag=true;
                                    }
                                }
                            }
                            long duration=System.nanoTime()-startTime;
                            System.out.println("Sequential serch in the eventbus has been ended in: "+duration+" nsec\n");
                            System.out.println("\nMessages sent for topic (" + topicName + ") to subscriber " + subIP + "..!\n");
                        } else {
                            System.out.println("\nNo messages retrieved for topic (" + topicName + ")..!\n");
                        }
                    }
                }
                    System.out.println("Subscriber " + subIP + " has been disconnected..!");
                }
            }
            conn.close();
            //////////////////////////////////////////////////////////////
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }


}
