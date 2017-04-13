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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
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
    int listeningPort, publishTopicPort = 60000, publishMessagePort = 60001,SubscribtionRequest = 60002, subscriberPullRequest=60003;    //each port hold a deffirent function
    static int maxsize = 0;
    /* create a hash map table that holds a concurrent hash map to assure synchronization
    *  each hash element contains a string ID (file name) and array of Messages*/
    static volatile Map<String, List<Message>> indexBus = new ConcurrentHashMap<String, List<Message>>();
    
    static volatile Map<String, Set<String>> topicSubscibtionList = new ConcurrentHashMap<String, Set<String>>();

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
        	Subscribe_Topic_Request();
        else if (listeningPort == subscriberPullRequest)        //call Register_a_File() if its port is connected with a peer
        	subscriberPullRequest();
    }

    /*********************************************************************************************/

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
                    	topicLog(messageMarker, "topic");
                 
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

    synchronized void Subscribe_Topic_Request() {
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
                	//subModel.setPort(port);
                	subModel.setIP(subIP);
                	subModel.setTopicName(topicName);
                	Set<String>  list = topicSubscibtionList.get(subModel.getTopicName());
                	if(list == null){
                		list = new HashSet<>();
                		
                	}
                	list.add(subIP);
                	//topicSubscibtionList.put(subModel.getTopicName(),list);
                	//////////////////////////////////////////////////////////////
                	if(topicSubscibtionList.containsKey(topicName))
                	{
                		reply="You are already subcribed to "+topicName+" !\n";
                	}
                	else
                	{
                		Subscription_Recorder(subIP+"-"+recievedString);
                		topicSubscibtionList.put(subModel.getTopicName(),list);
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
    
    public void topicLog(MessageMarker message, String type)
    {
    	String path=null;
    	if(type.equals("topic"))
    	{
    		path="/Topic_Log.txt";
    	}
    	else if (type.equals("message"))
    	{
    		path="/message_Log.txt";
    	}
    	FileOutputStream file;
    	File folder = new File(".");
		try {
			file = new FileOutputStream(folder+path,true);
		
    	ObjectOutputStream writer = new ObjectOutputStream(file);
    	writer.writeObject(message);
    	
    	
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void Subscription_Recorder(String record) //write the downloaded file into the local director
    {
        try
        {
            //final File f = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().getPath()); //get the jar directory
            //File parentFolder = new File(f.getParent());                                     //get the parent folder of the jar
            //File folder = new File(parentFolder.getParent()+"/peer1/src/main/resources"); //get the resources folder path
        	File folder = new File(".");
            FileWriter writer = new FileWriter(folder+"/Subscribtion_Records.txt",true);//initiate writer
            /////////////////////////////////////////////////////////////////////////////
            writer.write(record+"\n"); 
            //write
            writer.close();                                           //close writer
        }
        catch(UnknownHostException unknownHost){                      //To Handle Unknown Host Exception
            System.err.println("host not available..!");
        }
        catch(IOException ioException){                               //To Handle Input-Output Exception
            ioException.printStackTrace();
        }
        catch(Exception e){
            e.printStackTrace();
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
            	if(messageList != null){
            		messageList.add(m);
            	}
            	indexBus.put(topicNameStr, messageList);
            	if(m.isDurable())  //only durable messaged gets persisted
            		topicLog(m, "message");
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
            System.out.println("Type the action number as following:");
            System.out.println("1. To exit.\n");
            Thread.currentThread().stop();
        }
    }

	public static void prepareEventBus() {
		String topicObjectPath="/Topic_Log.txt";
		String messageObjectPath="/message_Log.txt";
		String subscriptionObjectPath="/Subscribtion_Records.txt";
    	FileInputStream fileTopic;
    	FileInputStream fileMessage;
    	FileInputStream fileSub;
    	ObjectInputStream topicReader = null,messageReader = null , subscriptionReader = null;
    	File folder = new File(".");
		try {
			
			fileTopic = new FileInputStream(folder+topicObjectPath);			
			topicReader = new ObjectInputStream(fileTopic);
			Object objTopic,ObjMesg,subObj;
			try{
				while(( objTopic = topicReader.readObject())!=null){
					TopicModel topicObject =(TopicModel) objTopic;
					indexBus.put(topicObject.getTopicName(), new ArrayList<Message>());
				}
			}catch (IOException e) {
				
			}
			
			fileMessage = new FileInputStream(folder+messageObjectPath);			
			messageReader = new ObjectInputStream(fileMessage);
			try{
				while(( ObjMesg = messageReader.readObject())!=null){
					Message msgObject =(Message) ObjMesg;
					List<Message> tpList = indexBus.get(msgObject.getTopicName());
					tpList.add(msgObject);
					indexBus.put(msgObject.getTopicName(),tpList);
				}
			}catch (IOException e) {
				
			}
			
			/*fileSub = new FileInputStream(folder+subscriptionObjectPath);			
			subscriptionReader = new ObjectInputStream(fileSub);
			try{
				while(( subObj = subscriptionReader.readObject())!=null){
					SubscribtionModel subObject =(SubscribtionModel) subObj;
					Set<String> subList = topicSubscibtionList.get(subObject.getTopicName());
					subList.add(subObject.getIP());
					topicSubscibtionList.put(subObject.getTopicName(),subList);
				}
			}catch (IOException e) {
				
			}*/
			System.out.println(" printing recreated index bus");
			for(Map.Entry<String, List<Message>> entry : indexBus.entrySet()){
				System.out.println("Topic name "+entry.getKey() +" conatains below message");
				for(Message m : entry.getValue()){
					System.out.println(m.getData());
				}
			}
    	
		} catch (FileNotFoundException e) {
				System.out.println("Event Bus is about to be created..\n");
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
    
    
}
