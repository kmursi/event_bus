package com.aos.pubsub.services.components;


import java.io.IOException;
import java.net.InetAddress;
import java.util.Scanner;

import com.aos.pubsub.services.model.Message;
import com.aos.pubsub.services.model.MessageMarker;
import com.aos.pubsub.services.model.TopicModel;

/*********************************************************************************************/
public class Main extends Thread{
    static int port;
    Main(int port)
    {
        this.port = port;
    }
    /*********************************************************************************************/
    public static void main(String[] args) throws InterruptedException {
        String userInput,serverIP,topicToPubSub;                   //define user input variable
        System.out.println("Enter the Indexing Server IP:");
        Scanner uIn = new Scanner(System.in);
        serverIP=uIn.nextLine().trim();
      //  serverIP = args[0];
       // = args[1];
        /////////////////////////////////////////////////////////////////////////////
        try {
            // Check if the server is up !
            if(InetAddress.getByName(serverIP).isReachable(5000))
                System.out.println("Server is up..!");
            else
            {
                System.out.println("Server not found..!");
                System.out.println("Exiting..");
                System.exit(0);
            }
        } catch (IOException ex) {
            System.out.println("Server not found..!");
            System.out.println("Exiting..");
            System.exit(0);
        }
        /////////////////////////////////////////////////////////////////////////////
        String peerID = "60006";             // the peer ID used as a port listener as well
        Thread thread;                      //define thread
        System.out.println("\nWaiting for messages..");
        System.out.println("=======================================================\n");
        /////////////////////////////////////////////////////////////////////////////
        try
        {
            thread = new Thread (new Main(Integer.parseInt(peerID))); //initiate listener thread
            thread.start();                             //start listener
        } catch(Exception e){                           //track general errors
            e.printStackTrace();
        }
        /////////////////////////////////////////////////////////////////////////////
        MessageHandler fh = new MessageHandler(peerID,serverIP);     //define and initiate handler object from the main thread
        /////////////////////////////////////////////////////////////////////////////
        while (true)
        {
            //Printing the available services
            System.out.println("*********************************************************************************************");
            System.out.println("Type the action number as following:");
            System.out.println("1. Register a topic with eventbus.");
            System.out.println("2. Register a message in topic");
            System.out.println("3. Subscription request");
            System.out.println("4. To exit.");
            System.out.println("5. Pull request");
            System.out.println("*********************************************************************************************\n");
            Scanner in = new Scanner(System.in);
            userInput = in.nextLine();                         //get the chosen service from the user
           
             if (userInput.equals("1"))                    //if user entered 2
            {
                System.out.println("Enter the topic name");
                String topicName = in.nextLine();                     // get file name that user want to register
            	 System.out.println("Registering the topic "+topicName);
                MessageMarker mm = null;
                TopicModel tModel = new TopicModel();
                tModel.setTopicName(topicName);
               // tModel.setTopicName(topicToPubSub);
                mm = tModel;
              //  tModel.setTopicName(topicName);
                tModel.setDurable(true);
                fh.publishTopic(mm);             //call register function and attach the file name
            }
            /////////////////////////////////////////////////////////////////////////////
            else if (userInput.equals("2"))                    //if user entered 3
            {
            	 System.out.println("Enter the topic name");
                 String topicName = in.nextLine();                     // get file name that user want to register
               //  System.out.println("Enter the message ");
              //   String messageStr = in.nextLine();  
                 MessageMarker mm = null;
                 Message message = new Message(0,"",topicName);
               //  tModel.setTopicName(topicName);
                 fh.publishMessage(message);             //call register function and attach the file name
            }
           
            else if (userInput.equals("3"))                    //if user entered 6
            {
            	System.out.println("Enter the topic name");
                String topicName = in.nextLine();  
                fh.Subscribe_Request(topicName);
            }
            else if (userInput.equals("4"))                    //if user entered 6
            {
                System.out.println("Exiting...");
                System.exit(0);                         //exit the program
            }
            else if (userInput.equals("5"))                    //if user entered 6
            {
            	System.out.println("Enter the topic name");
                String topicName = in.nextLine();  
                System.out.println("Enter the last index");
                String lastMessageIndex = in.nextLine();
                fh.pullRequest(topicName, Integer.parseInt(lastMessageIndex)); //exit the program
            }
            else
            {
                // awareness for the user of the correct options
                System.out.println("Wrong input! the input should be 1, 2, 3, or 4 ..\n");
            }
        }
    }


}




