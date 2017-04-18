//tests
package com.aos.pubsub.services.eventBus;
import java.util.Date;
import java.util.List;
import java.io.File;
//------------
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.aos.pubsub.services.model.Message;
public class Main extends Thread{

    int port;
    static final File f = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().getPath()); //get the jar directory
    static File temp = new File(f.getParent());
    static File parentFolder = new File(temp.getParent());
    static String topicObjectPath="/Topic_Log.txt";
    static String messageObjectPath="/message_Log.txt";
    static String subscriptionObjectPath="/Subscribtion_Records.txt";
    static ThreadPoolExecutor executor;
    Main(int port)                                      //Main constructor that receive port number
    {
        this.port = port;
    }
    public static void main(String[] args) throws IOException {
    	
    	System.out.println("=======================================================\n");
        System.out.println("Preparing Event Bus server..........\n");
        EventBusListener.prepareEventBus(); // recover durable topics and message from disk
       
        System.out.println("Event Bus is up and running..........\n");
        System.out.println("=======================================================\n");
        System.out.println("Retrieving subscribers list..........\n");
        EventBusListener.prepareSubscriptionList(); // recover durable topics and message from disk
        System.out.println("Subscribers list is up and running..........\n");
        System.out.println("=======================================================\n");
        
        /////////////////////////////////////////////////////////////////////////////
        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        /////////////////////////////////////////////////////////////////////////////
        Thread t1 = new Thread (new Main(60000));   //Create a thread that Listen for registration requests from peers
        t1.start();                                     //Start thread
        /////////////////////////////////////////////////////////////////////////////
        Thread t2 = new Thread (new Main(60001));   //Create a thread that Listen for registration requests from peers
        t2.start();                                     //Start thread
        /////////////////////////////////////////////////////////////////////////////
        Thread t3 = new Thread (new Main(60002));   //Create a thread that Listen for registration requests from peers
        t3.start();                                     //Start thread
        Thread t4 = new Thread (new Main(60003));   //Create a thread that Listen for registration requests from peers
        t4.start();                                     //Start thread
        Thread t5 = new Thread (new Main(60004));   //Create a thread that Listen for registration requests from peers
        t5.start();
        Thread t6 = new Thread (new Main(0));   //Create a thread that Listen for registration requests from peers
        t6.start();   
        while (true)
        {
            System.out.println("Type the action number as following:");
            System.out.println("1. To exit.");
            Scanner in = new Scanner(System.in);
            String userInput = in.nextLine().trim();
            if (userInput.equals("1"))                         //if user entered 6
            {
                System.out.println("Exiting...");
                System.exit(0);                         //exit the program
            }
        }
    }

		
	/*********************************************************************************************/

    public synchronized void run(){
    	
    	if(port==0)
    	{
    		System.out.println("Running the garbage collector thread..!\n");
    		grbageCollector();
    	}
    	else{
        try {
        	System.out.println("PORT "+port);
            ServerSocket ssock = new ServerSocket(port); // create a socket for both search or register listeners
            while (true) {                                 //keep listening for peers
                Socket sock = null;
                sock = ssock.accept();                     //accept peer connection
                EventBusListener listener = new EventBusListener(sock,port);
                executor.execute(listener);
                //new Listener(sock,port).start();         // create new thread
            }
        }
        catch(UnknownHostException unknownHost){                                           //To Handle Unknown Host Exception
            System.err.println("host not available..!");
        }
        catch (IOException e) {                          //catch the I/O errors
            e.printStackTrace();
        }
        catch (Exception e) {                          //catch the general errors
            e.printStackTrace();
        }
    	}
    }
    
    public void grbageCollector()
    {
    	while(true)
    	{
    	Message m;
    	long currentTime = new Date().getTime();
    	for(Entry<String, List<Message>> entry : EventBusListener.indexBus.entrySet()) {
    	    String key = entry.getKey();
    	    List<Message> list = entry.getValue();
    	    for(int i=0;i< list.size();i++)
    	    {
    	    	m=list.get(i);
    	    	if(currentTime>m.getExpirationDate())
    	    	{
    	    		System.out.println("Message "+EventBusListener.indexBus.get(key).get(i).getId()+" !has been deleted from the main EventBus due to its expiration date !\n");
    	    		EventBusListener.indexBus.get(key).remove(i);
    	    			
    	    	}
    	    }
    	    
    	    for(Entry<String, List<Message>> e : EventBusListener.indexBus.entrySet()) {
        	    String key2 = e.getKey();
        	    List<Message> durableList = e.getValue();
        	    for(int i=0;i< durableList.size();i++)
        	    {
        	    	m=durableList.get(i);
        	    	if(currentTime>m.getExpirationDate())
        	    	{
        	    		System.out.println("Message "+EventBusListener.durableIndexBus.get(key2).get(i).getId()+" has been deleted from the durable messages queue due to its expiration date !\n");
        	    		EventBusListener.durableIndexBus.get(key2).remove(i);
        	    		
        	    	}
        	    }
        	    
        	   new EventBusListener().topicLog();
        	   
    	    // do what you have to do here
    	    // In your case, an other loop.
    	    }
    	}
    	try {
			sleep(50000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	System.out.println("Garbage collector finished cleaning the EvenetBus !\n");
    	}
    }
}
