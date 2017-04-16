package com.aos.pubsub.services.eventBus;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.aos.pubsub.services.model.Message;
import com.aos.pubsub.services.model.MessageMarker;
import com.aos.pubsub.services.model.SubscribtionModel;
import com.aos.pubsub.services.model.TopicModel;



/**
 * Created by kmursi on 3/10/17.
 */
public class EventBusListener extends Thread {
    Socket conn;
    ObjectMapper mapper = new ObjectMapper();
    int listeningPort, publishTopicPort = 60000, publishMessagePort = 60001,SubscribtionRequest = 60002, subscriberPullRequest=60003, messagesByTime=60004;    //each port hold a deffirent function
    static int maxsize = 0;
    /* create a hash map table that holds a concurrent hash map to assure synchronization
    *  each hash element contains a string ID (file name) and array of Messages*/
    static volatile Map<String, List<Message>> indexBus = new ConcurrentHashMap<String, List<Message>>();
    static volatile Map<String, List<Message>> durableIndexBus = new ConcurrentHashMap<String, List<Message>>();
    static volatile Map<String, Set<SubscribtionModel>> topicSubscibtionList = new ConcurrentHashMap<String, Set<SubscribtionModel>>();
	private ObjectOutputStream writer;

    /*********************************************************************************************/

    public EventBusListener(Socket s, int port) {
        conn = s;                                       // let the local socket to have the value of the received one
        this.listeningPort = port;                      // let the local port to have the value of the received one
    }

    /*********************************************************************************************/

    public synchronized void run() {
        if (listeningPort == publishTopicPort)           //call Register_a_File() if its port is connected with a peer
        	receivingTopicRequest();
            /////////////////////////////////////////////////////////////////////////////
        else if (listeningPort == publishMessagePort)        //call Register_a_File() if its port is connected with a peer
        	receivingMessage();
        else if (listeningPort == SubscribtionRequest)        //call Register_a_File() if its port is connected with a peer
        	SubscribeTopicRequest();
        else if (listeningPort == subscriberPullRequest)        //call Register_a_File() if its port is connected with a peer
        	subscriberPullRequest();
        else if (listeningPort == messagesByTime)        //call Register_a_File() if its port is connected with a peer
        	getMessagesBasedOnTime();
    }

    /*********************************************************************************************/
    synchronized void checkMessagesValidit()
    {
    	while(true)
    	{
    		
    	}
    }
    synchronized void receivingTopicRequest() {
        try {
            String topicName;                             // define an integer peer ID which is the  peer port
            List<Message> messageList;                    //String array used for splitting the received message
            /////////////////////////////////////////////////////////////////////////////
            String pubIP = conn.getInetAddress().getHostName();    //save the peer IP into peerIP
            ObjectInputStream in = new ObjectInputStream(conn.getInputStream()); //initiate object input stream to read from peer
            MessageMarker messageMarker;
            String recievedString = null;
            
            try{
            	recievedString = (String) in.readObject();               //read
            	messageMarker = mapper.readValue(recievedString, TopicModel.class);
           }catch(JsonMappingException  | JsonParseException jEx){
        	   messageMarker =  mapper.readValue(recievedString, Message.class);
           }
            TopicModel topic = null;
            Message messageModel = null;
            
            if(messageMarker instanceof TopicModel){
            	topic = (TopicModel) messageMarker;
            	long createdDate = new Date().getTime();
                topic.setCreatedOn(createdDate);
                topic.setUpdatedOn(createdDate);
                messageList = topic.getMessageList(); 
                topicName = topic.getTopicName();   
                if(messageList == null){
                	messageList = new ArrayList<Message>();
                	topic.setMessageList(messageList);
                 }  
              /*  for(int index = 0 ; index < messageList.size() ; index++){
                	Message m = new Message(index, messageList.get(index).getData(),topicName);
                	topic.getMessageList().add(m);
            	 }*/
                	System.out.println("Topic " + topicName + "  created in the event bus \n");
                    /////////////////////////////////////////////////////////////////////////////
                    indexBus.put(topicName, messageList);             //store the hashmap element
                    
                    if(topic.isDurable())							  // log only when topic is durable
                    {
                    	durableIndexBus.put(topicName, messageList);
                    	topicLog();
                    }
                 
            }else{
            	System.out.println("Invalid object passed . returning....");
            	
            }
            
            /////////////////////////////////////////////////////////////////////////////
                  //split the incoming message to adapt the local format

             		// store peer ID
            
                in.close();                                         //close reader
                conn.close();                                       //close connection
            }
        /////////////////////////////////////////////////////////////////////////////
        catch(UnknownHostException unknownHost){                                           //To Handle Unknown Host Exception
            System.err.println("host not available..!");
        }
        catch(IOException ioException){                                                    //To Handle Input-Output Exception
            ioException.printStackTrace();
        }
         catch (Exception e) {                                      //track general errors
            e.printStackTrace();
            System.out.println(e.toString());
        }

        finally {
            System.out.println("Type the action number as following:");
            System.out.println("1. To exit.\n");
            Thread.currentThread().stop();
        }
    }

    /*********************************************************************************************/

    synchronized void SubscribeTopicRequest() {
        try{
            String topicName,reply=null;                             // define an integer peer ID which is the  peer port
            List<Message> messageList;
            int peerID;
            //String array used for splitting the received message
            /////////////////////////////////////////////////////////////////////////////
            String subIP = conn.getInetAddress().getHostName();    //save the peer IP into peerIP
            ObjectInputStream in = new ObjectInputStream(conn.getInputStream()); //initiate object input stream to read from peer
           // MessageMarker messageMarker;
            String recievedString = null;
            recievedString=(String)in.readObject();
            if(!recievedString.equals(null))
            {
            	String messageArray [] = recievedString.split("-");
            	peerID = Integer.parseInt(messageArray[0]);
            	topicName = messageArray[1];
            	if(indexBus.containsKey(topicName))
            	{
                	////////////////////////////////////////////////////////////
                	SubscribtionModel subModel = new SubscribtionModel();
                	subModel.setPort(peerID);
                	subModel.setIP(subIP);
                	subModel.setTopicName(topicName);
                	Set<SubscribtionModel>  list = topicSubscibtionList.get(subModel.getTopicName());
                	if(list == null){
                		list = new HashSet<>();
                		
                	}
                	list.add(subModel);
                	//topicSubscibtionList.put(subModel.getTopicName(),list);
                	//////////////////////////////////////////////////////////////
                	if(topicSubscibtionList.containsKey(topicName))
                	{
                		reply="You are already subcribed to "+topicName+" !\n";
                	}
                	else
                	{
                		topicSubscibtionList.put(subModel.getTopicName(),list);
                		Subscription_Recorder();
                		reply="You are now subcribing topic '"+topicName+"'";
                		System.out.println("Subscribtion request from "+subIP+":"+peerID+" accepted for topic "+topicName+"\n");
                	}
            	}
            	else
                {
                	System.out.println("No message Received!\n");
                	reply = "Topic not found\n";  
                }
            }
            else
            {
            	System.out.println("No message Received!\n");
            	reply = "Topic not found\n";  
            }
            ObjectOutputStream out = new ObjectOutputStream(conn.getOutputStream()); //define object writer
            out.writeObject(reply);                                                  //write the reply to the peer
            out.flush();
            in.close();   
            out.close();
            conn.close();   
        }
        catch(Exception e)
        {
        	
        }  
    }
    
    public void subscriberPullRequest()
    {
    	//System.out.println("\nhi\n");
    	new SubscriberHandler(conn,listeningPort).start();
    	//System.out.println("\nhi\n");
    }
    
    public void topicLog()
    {
    	FileOutputStream file=null;
    	File folder = Main.parentFolder;
		try {
			file = new FileOutputStream(folder+Main.topicObjectPath);
    	writer = new ObjectOutputStream(file);
    	writer.writeObject(durableIndexBus);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    }
    
    public void Subscription_Recorder() //write the downloaded file into the local director
    {
        FileOutputStream file=null;
    	File folder = Main.parentFolder;
		try {
			file = new FileOutputStream(folder+Main.subscriptionObjectPath);
    	writer = new ObjectOutputStream(file);
    	writer.writeObject(topicSubscibtionList);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    }
    
    synchronized void receivingMessage() {
        try {
            String topicName;                             // define an integer peer ID which is the  peer port
            List<Message> messageList;                    //String array used for splitting the received message
            /////////////////////////////////////////////////////////////////////////////
            String pubIP = conn.getInetAddress().getHostName();    //save the peer IP into peerIP
            ObjectInputStream in = null;
            while(conn.getInputStream().available() != -1){
            	in = new ObjectInputStream(conn.getInputStream()); //initiate object input stream to read from peer
            	MessageMarker messageMarker;
            	String recievedString = null;
            
            try{
            	recievedString = (String) in.readObject();               //read
            	messageMarker = mapper.readValue(recievedString, TopicModel.class);
           }catch(JsonMappingException  | JsonParseException jEx){
        	   messageMarker =  mapper.readValue(recievedString, Message.class);
           }
            TopicModel topic = null;
            Message messageModel = null;
            
            if(messageMarker instanceof Message){
            	messageModel = (Message)messageMarker;
            	String topicNameStr = messageModel.getTopicName();
            	messageList  = indexBus.get(topicNameStr);
            	Message m = new Message(messageList.size(), messageModel.getData(),topicNameStr );
            	m.setDurable(true);
            	m.setCreatedOn(messageModel.getCreatedOn());
            	if(messageList != null){
            		messageList.add(m);
            	}
            	indexBus.put(topicNameStr, messageList);
            	if(m.isDurable())  //only durable messaged gets persisted
            	{
            		durableIndexBus.put(topicNameStr, messageList);
            		topicLog();
            	}
            	System.out.println("Added new message  "+messageModel.getData() + " in topic "+topicNameStr );
            }else{
            	System.out.println("Invalid object passed . returning....");
            	
            }
            
            /////////////////////////////////////////////////////////////////////////////
                  //split the incoming message to adapt the local format

             		// store peer ID
           }
                in.close();                                         //close reader
                conn.close();                                       //close connection
            }
        /////////////////////////////////////////////////////////////////////////////
        catch(EOFException eof){
    	   System.out.println("finished publishing topics");
       }
        catch(UnknownHostException unknownHost){                                           //To Handle Unknown Host Exception
            System.err.println("host not available..!");
        }
        catch(IOException ioException){                                                    //To Handle Input-Output Exception
            ioException.printStackTrace();
        }
         catch (Exception e) {                                      //track general errors
            e.printStackTrace();
            System.out.println(e.toString());
        }

        finally {
            System.out.println("\nType the action number as following:");
            System.out.println("1. To exit.\n");
            Thread.currentThread().stop();
        }
    }

	public static void prepareEventBus() throws IOException {
		String topicObjectPath= Main.topicObjectPath;
    	FileInputStream fileTopic;
    	ObjectInputStream topicReader = null,messageReader = null ;
    	File folder = Main.parentFolder;
    	//System.out.println("path"+folder.toString());
    	if(Files.size(Paths.get(folder+topicObjectPath))>0)
    	{
			fileTopic = new FileInputStream(folder+topicObjectPath);
			topicReader = new ObjectInputStream(fileTopic);
			try{
				while(true){
					//objTopic = 
						Map topicObject = (Map)topicReader.readObject();
					indexBus= topicObject;
					durableIndexBus=topicObject;	
				}
			}
			catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (IOException e) {
				
			}
			try{
			System.out.println("*********************************************************************************************");
			System.out.println("Printing recreated index bus..!");
			for(Map.Entry<String, List<Message>> entry : indexBus.entrySet()){
				System.out.println("\nTopic name "+entry.getKey() +" conatains below message:");
				for(Message m : entry.getValue()){
					System.out.println(m.getData());
				}
			}
			System.out.println("*********************************************************************************************");
    	
		}
		catch(Exception e){
			e.printStackTrace();
		}finally {
			try {
				if(messageReader != null )
					messageReader.close();
				if(topicReader != null )
					topicReader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			}
		}
    	else
    	{
    		System.out.println("\nThis server has no previously recorded topics..!\n");
    	}
		
	}
	
	public static void prepareSubscriptionList() throws IOException {
		String subscribersObjectPath= Main.subscriptionObjectPath;
    	FileInputStream subscriptionInput;
    	ObjectInputStream subscriptionReader = null,messageReader = null ;
    	File folder = Main.parentFolder;
    	if(Files.size(Paths.get(folder+subscribersObjectPath))>0)
    	{
    		subscriptionInput = new FileInputStream(folder+subscribersObjectPath);
    		subscriptionReader = new ObjectInputStream(subscriptionInput);
    		Map<String, Set<SubscribtionModel>> readObject=null;
			try{
				while(true){
					//objTopic = 
						//Map topicObject = (Map)subscriptionReader.readObject();
						readObject = (Map<String, Set<SubscribtionModel>>) subscriptionReader.readObject();

						topicSubscibtionList= readObject;	
				}
			}
			catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (IOException e) {
				
			}
			try{
			System.out.println("*********************************************************************************************");
			System.out.println("Printing recreated topic subscibtion list..!");
			for(Map.Entry<String, Set<SubscribtionModel>> entry : topicSubscibtionList.entrySet()){
				System.out.println("\nTopic name "+entry.getKey() +" conatains the following subscribers:");
				for(SubscribtionModel m : entry.getValue()){
					System.out.println("Subscriper IP:"+m.getIP()+", Subscriber port: "+m.getPort()+"\n");
				}
			}
			System.out.println("*********************************************************************************************");
    	
		}
		catch(Exception e){
			e.printStackTrace();
		}finally {
			try {
				if(messageReader != null )
					messageReader.close();
				if(subscriptionReader != null )
					subscriptionReader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			}
		}
    	else
    	{
    		System.out.println("\nThis server has no previously recorded subscribers..!\n");
    	}
		
	}
	
	public synchronized void getMessagesBasedOnTime()
	{
		try 
		{
			//socket = new ServerSocket(port);
			String receivedMessage, topicName;
			Message message;
			ObjectOutputStream out;
			String subIP=conn.getInetAddress().getHostName();
			Socket socket;
			Long lastMessageDate;
			List<Message> subscriberMessage =null;
			//while(true)
			{
				ObjectInputStream in = new ObjectInputStream(conn.getInputStream());
				out = new ObjectOutputStream(conn.getOutputStream());
				if(!(receivedMessage = (String)in.readObject()).equals(null))
				{
				//System.out.println("\nhi splitter\n");
				String splitter [] = receivedMessage.split("-");
				topicName=splitter[0].trim();
				//DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm",Locale.US);
				lastMessageDate = Long.parseLong(splitter[1].trim());
				/////////////////////////////////////////////////////////////////////////
				
				if(conn.isConnected())
				{
					if(indexBus.containsKey(topicName))
					{
					subscriberMessage = EventBusListener.indexBus.get(topicName);
						subscriberMessage = EventBusListener.indexBus.get(topicName);
						for(int i=0; i<subscriberMessage.size();i++)
						{
							message=subscriberMessage.get(i);
							//long milliseconds = lastMessageDate.getTime();
							//System.out.println("message.getCreatedOn() "+message.getCreatedOn()+">lastMessageDate"+ lastMessageDate);
							if(message.getCreatedOn()>lastMessageDate)
							{
							//System.out.println(message.getTopicName());
							
							
							//pushToSubscriber(message);
							//System.out.println("\nConnected to the subscriber..\n");
				              //initiate writer
				            out.flush();
				            out.writeObject(mapper.writeValueAsString(message));                                 //send the message
				            out.flush();
							}
						}
						System.out.println("\nMessages sent for topic ("+topicName+") to subscriber "+subIP+"..!\n");
					}
					else
					{
						System.out.println("\nNo messages retrieved for topic ("+topicName+")..!\n");
					}
				}
				
				//System.out.println("Subscriber "+subIP+":"+port+" received messaeges in "+(msgRecievingEndTime - msgRecievingStartTime) +" milliseconds" );
				System.out.println("Subscriber "+subIP+" has been disconnected..!");
			  }
			}
			conn.close();
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally
		{
			try {
				conn.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				
			}
		}
		
	}
    
    
}