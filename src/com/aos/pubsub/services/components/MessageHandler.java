package com.aos.pubsub.services.components;


import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Random;

import org.codehaus.jackson.map.ObjectMapper;

import com.aos.pubsub.services.model.Message;
import com.aos.pubsub.services.model.MessageMarker;
import com.opencsv.CSVReader;

public class MessageHandler {
	private ObjectMapper mapper = new ObjectMapper();
    String peerID;
    String serverIP = "localhost";
    ObjectOutputStream out;
    Socket socket;
    /*********************************************************************************************/

    public MessageHandler (String peerID, String serverIP)
    {
        this.serverIP=serverIP;
        this.peerID=peerID;             //store peer ID into peerID
    }

    /*********************************************************************************************/

    public void publishTopic(MessageMarker topicModel)                      //Register with index server Method
    {
        /////////////////////////////////////////////////////////////////////////////
    	try {
                socket = new Socket(serverIP, 60000);                //connect to the registration socket on the server
                System.out.println("\nConnected to the server..\n");
                out = new ObjectOutputStream(socket.getOutputStream());   //initiate writer
                out.flush();
                out.writeObject(mapper.writeValueAsString(topicModel));                                 //send the message
                out.flush();
                /////////////////////////////////////////////////////////////////////////////
              //  System.out.println("Topic '"+topicModel.getTopicName()+"'  has been published successfully on the server!!\n");
                out.close();                                               //close writer
                socket.close();                                            //close socket
                System.out.println("\nYour topic has been registered on the eventBus ! \n");
            }
            catch(UnknownHostException unknownHost){                       //To Handle Unknown Host Exception
                System.err.println("host not available..!");
            }
            catch(Exception e ){                                           //To Handle Input-Output Exception
                e.printStackTrace();
            }
    }
    
    public void publishMessage(MessageMarker messageModel)                      //Register with index server Method
    {
        /////////////////////////////////////////////////////////////////////////////
    	try {
    		
                socket = new Socket(serverIP, 60001);                //connect to the registration socket on the server
                System.out.println("\nConnected to the server..\n");
                
                Message m1 = (Message) messageModel;
              //  while ((line = reader.readNext()) != null) {
                Random ran = new Random();
                Date dt = new Date();
                int counter = 0;
                while (counter < 10000) {
                   Message m = new Message();
                   m.setTopicName(m1.getTopicName());
                   m.setDurable(true);
                   m.setData(dt.toString()+"_"+ran.nextInt());
                   out = new ObjectOutputStream(socket.getOutputStream());   //initiate writer
                   out.flush();
                   out.writeObject(mapper.writeValueAsString(m));                                 //send the message
                   out.flush();
                   counter++;
                }
               System.out.println(counter);
                /////////////////////////////////////////////////////////////////////////////
              //  System.out.println("Topic '"+topicModel.getTopicName()+"'  has been published successfully on the server!!\n");
                out.close();                                               //close writer
                socket.close();                                            //close socket
            }
            catch(UnknownHostException unknownHost){                       //To Handle Unknown Host Exception
                System.err.println("host not available..!");
            }
            catch(Exception e ){                                           //To Handle Input-Output Exception
                e.printStackTrace();
            }
    }


    /*********************************************************************************************/


    public void Subscribe_Request(String topicName)                  //search file on the server
    {
        String message;
        ObjectInputStream in;
        try{
            socket = new Socket(serverIP, 60002);              //initiate socket withe the server through server searching port
            System.out.println("\nConnected to the server..\n");
            /////////////////////////////////////////////////////////////////////////////
            
            out = new ObjectOutputStream(socket.getOutputStream());//initiate writer
            out.flush();
            message = peerID+"-"+topicName;
            out.writeObject(message);                        //send
            out.flush();
            
		            in = new ObjectInputStream(socket.getInputStream());//initiate reader
		            message = in.readObject().toString();                  //store received message into message
		            /////////////////////////////////////////////////////////////////////////////
		            if  (message.trim().equals("Topic not found")) 
		            {        //check the file index existence in the server based on the server message
		
		                System.out.println("Topic not found !\n");
		            }
		            else 
		            {
		                //System.out.println( "Your subscribing request to topic ("+topic+ ") succeeded \n"); //list the peers who have the file
		                System.out.print(message+"\n");
		                pullRequest(topicName, 0);
		            }
            	
            /////////////////////////////////////////////////////////////////////////////
            
            //in.close();                                            //close reader
            //out.close();                                           //close writer
            //socket.close();                                        //close connection
        }
        catch(UnknownHostException unknownHost){                   //To Handle Unknown Host Exception
            System.err.println("host not available..!");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void pullRequest(String topicName, int lastMessageIndex)
    {
    	new Listener(serverIP,topicName, lastMessageIndex).start();
    }
    
    
    /*********************************************************************************************/

    public void Create_Local_File(String fileName,String fileContent) //write the downloaded file into the local director
    {
        try
        {
            final File f = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().getPath()); //get the jar directory
            File parentFolder = new File(f.getParent());                                     //get the parent folder of the jar
            File folder = new File(parentFolder.getParent()+"/peer1/src/main/resources"); //get the resources folder path

            FileWriter writer = new FileWriter(folder+"/"+fileName.trim(),true);//initiate writer
            /////////////////////////////////////////////////////////////////////////////
            writer.write(fileContent+"\n");                                //write
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

    /*********************************************************************************************/

}
