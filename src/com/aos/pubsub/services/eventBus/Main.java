//test
package com.aos.pubsub.services.eventBus;
import com.aos.pubsub.services.model.Message;
import com.aos.pubsub.services.model.SubscribtionModel;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/*********************************************************************************************/

public class Main extends Thread {
    int port;
    static final File f = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().getPath()); //get the jar directory
    static File temp = new File(f.getParent());
    static File parentFolder = new File(temp.getParent());
    static String topicObjectPath = "/Topic_Log.txt";
    static String messageObjectPath = "/message_Log.txt";
    static String subscriptionObjectPath = "/Subscribtion_Records.txt";
    static ThreadPoolExecutor executor;
    
    /*********************************************************************************************/
    
    Main(int port)                                      								//Main constructor that receive port number
    {
        this.port = port;
    }
    
    /*********************************************************************************************/
    
    public static void main(String[] args) throws IOException {

        System.out.println("=======================================================\n");
        System.out.println("Preparing Event Bus server..........\n");
        EventBusListener.prepareEventBus(); 											// recover durable topics and message from disk
        System.out.println("Event Bus is up and running..........\n");
        /////////////////////////////////////////////////////////////////////////////
        System.out.println("=======================================================\n");
        System.out.println("Retrieving subscribers list..........\n");
        EventBusListener.prepareSubscriptionList(); 									// recover subscribers list from disk
        System.out.println("Subscribers list is up and running..........\n");
        System.out.println("=======================================================\n");
        /////////////////////////////////////////////////////////////////////////////
        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();				//thread management pool
        /////////////////////////////////////////////////////////////////////////////
        Thread t1 = new Thread(new Main(60000));   										//Create a thread that Listen for publish topics requests from eventbus
        t1.start();                                     								//Start thread
        /////////////////////////////////////////////////////////////////////////////
        Thread t2 = new Thread(new Main(60001));   										//Create a thread that Listen for publish messages from eventbus
        t2.start();                                     								//Start thread
        /////////////////////////////////////////////////////////////////////////////
        Thread t3 = new Thread(new Main(60002));   										//Create a thread that Listen for subscription requests from eventbus
        t3.start();                                     								//Start thread
        /////////////////////////////////////////////////////////////////////////////
        Thread t4 = new Thread(new Main(60003));   										//Create a thread that Listen for pull requests from eventbus
        t4.start();                                     								//Start thread
        /////////////////////////////////////////////////////////////////////////////
        Thread t5 = new Thread(new Main(60004));   										//Create a thread that Listen for pull by time requests from eventbus
        t5.start();																		//Start thread
        /////////////////////////////////////////////////////////////////////////////
        Thread t6 = new Thread(new Main(0));   											//Create a thread that running the garbage collector  from eventbus
        t6.start();																		//Start thread
        /////////////////////////////////////////////////////////////////////////////
        while (true) {
            System.out.println("Type the action number as following:");
            System.out.println("1. To exit.\n");
            /////////////////////////////////////////////////////////////////////////////
            Scanner in = new Scanner(System.in);
            String userInput = in.nextLine().trim();
            if (userInput.equals("1"))                         							//if user entered 1
            {
                System.out.println("Exiting...");
                System.exit(0);                         								//exit the program
            }
        }
    }

    /*********************************************************************************************/

    public synchronized void run() {

        if (port == 0) {
        	EventBusListener listener = new EventBusListener( port);
            executor.execute(listener);		
        } else {
            try {
                System.out.println("PORT " + port);
                ServerSocket ssock = new ServerSocket(port); 							// create a socket for both search or register listeners
                while (true) {                                 							//keep listening for peers
                    Socket sock = null;
                    sock = ssock.accept();                     							//accept peer connection
                    EventBusListener listener = new EventBusListener(sock, port);
                    executor.execute(listener);											// create new thread
                }
            } catch (UnknownHostException unknownHost) {                                //To Handle Unknown Host Exception
                System.err.println("host not available..!");
            } catch (IOException e) {                          							//catch the I/O errors
                e.printStackTrace();
            } catch (Exception e) {                          							//catch the general errors
                e.printStackTrace();
            }
        }
    }

    /*********************************************************************************************/

}
