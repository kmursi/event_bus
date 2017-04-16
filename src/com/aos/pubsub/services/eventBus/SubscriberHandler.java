package com.aos.pubsub.services.eventBus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Date;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import com.aos.pubsub.services.model.Message;
import com.aos.pubsub.services.model.MessageMarker;
//


public class SubscriberHandler extends Thread {
	String IP , topicName;
	int port;
	ObjectOutputStream out;
	String subIP;
	Socket socket;
	int lastMessage;
	private ObjectMapper mapper = new ObjectMapper();
	List<Message> subscriberMessage ;
	public SubscriberHandler(Socket socket,int port)
	{
		this.port=port;
		subIP = socket.getInetAddress().getHostName(); 
		this.socket=socket;
	}
	public synchronized void run()
	{
		try 
		{
			//socket = new ServerSocket(port);
			String receivedMessage, topicName;
			Message message;
			int index=0;
			long time = new Date().getTime();
			//while(true)
			{
				ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
				out = new ObjectOutputStream(socket.getOutputStream());
				if(!(receivedMessage = (String)in.readObject()).equals(null))
				{
				//System.out.println("\nhi splitter\n");
				String splitter [] = receivedMessage.split("-");
				topicName=splitter[0].trim();
				subscriberMessage = EventBusListener.indexBus.get(topicName);
				lastMessage=getLastMessageIndex(time);
				System.out.println(lastMessage);
				/////////////////////////////////////////////////////////////////////////
				//long msgRecievingStartTime = new Date().getTime();
				while(socket.isConnected())
				{
					subscriberMessage = EventBusListener.indexBus.get(topicName);
					if(lastMessage<subscriberMessage.size()&&subscriberMessage.size()>0)
					{
						//System.out.println("if"+lastMessage);
						if(lastMessage==-1)
						{
							lastMessage=getLastMessageIndex(time);
							//System.out.println("condition"+lastMessage);
						}
						else
						{
							for(int i=lastMessage; i<subscriberMessage.size();i++)
							{
								message=subscriberMessage.get(i);
								//System.out.println(message.getTopicName());
								//pushToSubscriber(message);
								System.out.println("\nConnected to the subscriber..\n");
					              //initiate writer
					            out.flush();
					            out.writeObject(mapper.writeValueAsString(message));                                 //send the message
					            out.flush();
					            //index=i;
							}
							lastMessage = subscriberMessage.size();
						}
					}
				}
				//long msgRecievingEndTime = new Date().getTime();
				//System.out.println("Subscriber "+subIP+":"+port+" received messaeges in "+(msgRecievingEndTime - msgRecievingStartTime) +" milliseconds" );
				System.out.println("Subscriber "+subIP+":"+port+" has been disconnected..!");
			  }
			}
			socket.close();
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
				socket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	
	
	//This method handles the sending to the subscriber
	void pushToSubscriber( MessageMarker marker )
	{
		try
		{
			System.out.println("\nConnected to the subscriber..\n");
            out = new ObjectOutputStream(socket.getOutputStream());   //initiate writer
            out.flush();
            out.writeObject(mapper.writeValueAsString(marker));                                 //send the message
            out.flush();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	//get all the topics that a subscriber is subscribing
	String [] subscriptionList (String IP , int port)
	{
		BufferedReader br = null;
		FileReader fr = null;
		String resultArray [] = null;
		int counter=0;
		String splitter [];
		try {
			File folder = Main.parentFolder;
            //FileWriter writer = new FileWriter(folder+"/Subscribtion_Records.txt",true);
			File f = new File (folder+Main.subscriptionObjectPath);
			fr = new FileReader(folder+Main.subscriptionObjectPath);
			br = new BufferedReader(fr);
			resultArray= new String[(int)f.length()];
			String sCurrentLine;
			
			while ((sCurrentLine = br.readLine()) != null) {
				if(sCurrentLine.contains(IP+"-"+port))
				{
					splitter = sCurrentLine.split("-");
					resultArray [counter]= splitter[2];
				}
				counter++;
			}
				System.out.println(sCurrentLine);
			}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return resultArray;
	}
	
	
	public int getLastMessageIndex(long time)
	{
		
		Message message;
		for(int i=0; i<subscriberMessage.size();i++)
		{
			message=subscriberMessage.get(i);
			//System.out.println("message.getCreatedOn() "+message.getCreatedOn());
			//System.out.println("time "+time);
			if(message.getCreatedOn()>=time)
				return i;
		}
		return -1;
	}
}