import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;

public class DstoreControllerHandler extends Thread implements CommunicationSendListener {


    private Socket controllerSocket;
    private Dstore dStore;
    private int timeOut;
    private PrintWriter pw = null;
    private BufferedReader br = null;
    //private OutputStream out = null;
    //private InputStream in = null;
    private Logger logger = Logger.getLogger(String.valueOf(DstoreControllerHandler.class));
    private String fileFolder;

    public DstoreControllerHandler(Socket controllerSocket, Dstore dStore, int timeout, String fileFolder) {
        this.controllerSocket = controllerSocket;
        this.dStore = dStore;
        this.fileFolder = fileFolder;
        this.timeOut = timeOut;

        dStore.setControllerSendListener(this);
    }

    @Override
    public void run () {


        System.out.println( "[" + this.getName() + " Controller]" +  "Device Connected to Dstore Handler on Port: " + controllerSocket.getPort());

        try {
            // Set up Streams
            pw = new PrintWriter(controllerSocket.getOutputStream(), true);
            br = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));

            //in = controllerSocket.getInputStream();
            //out = controllerSocket.getOutputStream();

        } catch (IOException e) {
            e.printStackTrace();
        }

        //Delclare we are a DSTORE
        pw.println("JOIN " + dStore.port);


        // Waiting to receive message Loop
        String[] lineSplit = null;
        while (true) {
            String line = null;

            //System.out.println("-------------");
            //System.out.println(controllerSocket.isConnected());
            //System.out.println(controllerSocket.isClosed());

            try {
                if ((line = br.readLine()) != null) {

                    // Message Parsing
                    lineSplit = line.split(" ");
                    switch (lineSplit[0]) {


                        /*case "disconnect" -> {
                            System.out.println( "[" + this.getName() + " Controller]" +  "Message Received: " + line + " On Port: " + controllerSocket.getPort());
                            disconnect();
                            break;
                        }*/

                        case "LIST" -> {
                            System.out.println("[" + this.getName() + " Controller] " + " LIST COMMAND RECEIVED");


                            // include REMOVING files?
                            StringBuilder fileNames = new StringBuilder();
                            for (String fileName : dStore.files.keySet()) {
                                fileNames.append(fileName).append(" ");
                            }
                            pw.println("LIST " + fileNames.toString().trim());
                            System.out.println("[" + this.getName() + " Controller] " + "LIST COMPLETED: " + fileNames.toString().trim());

                        }

                        case "REMOVE" -> {
                            String fileName = lineSplit[1];

                            System.out.println( "[" + this.getName() + " Controller] " +  "REMOVE COMMAND RECEIVED FILE: " + fileName);
                            System.out.println( "[" + this.getName() + " Controller] " +  "CHECKING PATH: " + fileFolder + File.separator + fileName);
                            File fileToDelete = new File(fileFolder + File.separator + fileName);
                            if (!fileToDelete.exists()) {
                                pw.println("ERROR_FILE_DOES_NOT_EXIST " + fileName); // To Controller
                            } else {

                                while ( !fileToDelete.delete() ){}
                                pw.println("REMOVE_ACK " + fileName); // To Controller

                                dStore.files.remove(fileName);

                                System.out.println("[" + this.getName() + " Controller]" + "REMOVE: " + fileName + " COMPETE");

                            }
                        }

                        case "REBALANCE" -> {
                            System.out.println( "[" + this.getName() + " Controller] " +  "REBALANCE COMMAND RECEIVED: " + line);

                            var numFileToSend = Integer.parseInt(lineSplit[1]);

                            //ArrayList<String> toSendFileNames = new ArrayList<>();
                            //ArrayList<ArrayList<Integer>> toSendToDStores = new ArrayList<ArrayList<Integer>>();
                            HashMap<String, ArrayList<Integer>> fileToSendMap = new HashMap<>();

                            var offset = 2;

                            for (int i = 0; i < numFileToSend; i++) {
                                //toSendFileNames.add(lineSplit[i + offset]);
                                var fileName = lineSplit[i + offset];
                                offset++;


                                var numToSendTo = Integer.parseInt(lineSplit[i + offset]);
                                offset++;

                                var sendToArray = new ArrayList<Integer>();
                                for (int j = 0; j < numToSendTo; j++) {
                                    sendToArray.add(Integer.parseInt(lineSplit[i + offset + j]));
                                }
                                //toSendToDStores.add(sendToArray);
                                fileToSendMap.put(fileName, sendToArray);

                                offset += numToSendTo;
                            }

                            var toRemoveFileNames = new ArrayList<String>();

                            var numToRemove = Integer.parseInt(lineSplit[offset]);
                            offset++;

                            for (int i = 0; i < numToRemove; i++) {
                                toRemoveFileNames.add(lineSplit[offset + i]);
                            }








                            // Message Parsed now Act

                            // Send all files First, then Remove
                            var dStoreDidntRespondAck = false;

                            for ( String fileName : fileToSendMap.keySet() ) {
                                for ( Integer dStore : fileToSendMap.get(fileName) ) {

                                    // Connect and send file to DStore
                                    Socket dStoreSocket = null;
                                    PrintWriter dStorePW = null;
                                    BufferedReader dStoreBR = null;

                                    //InputStream dStoreIN = null;
                                    OutputStream dStoreOUT = null;

                                    try {
                                        // form connection with InPut and Output streams
                                        dStoreSocket = new Socket( "127.0.0.1", dStore);
                                        dStoreSocket.setSoTimeout(timeOut);


                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }

                                    try {
                                        // Set up Streams
                                        dStorePW = new PrintWriter(dStoreSocket.getOutputStream(), true);
                                        dStoreBR = new BufferedReader(new InputStreamReader(dStoreSocket.getInputStream()));

                                        dStoreOUT = dStoreSocket.getOutputStream();

                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }


                                    dStorePW.println("REBALANCE_STORE " + fileName + " " + Files.readAllBytes(Paths.get(fileFolder + File.separator + fileName)).length);

                                    try {
                                        String dStoreResponse;
                                        if ((dStoreResponse = dStoreBR.readLine()) != null) {

                                            if (dStoreResponse.equals("ACk")) {
                                                dStoreOUT.write(Files.readAllBytes(Paths.get(fileFolder + File.separator + fileName)));
                                            } else {
                                                dStoreDidntRespondAck = true;
                                            }

                                        } else {
                                            //DStore Timed out, Dont send REBALANCE_COMPLETE
                                            dStoreDidntRespondAck = true;
                                        }
                                    }catch (NullPointerException e){
                                        dStoreDidntRespondAck = true;
                                    }
                                }
                            }

                            // now remove files
                            if (!dStoreDidntRespondAck) {

                                for (String fileToRemove : toRemoveFileNames) {


                                    File fileToDelete = new File(fileFolder + File.separator + fileToRemove);
                                    if (!fileToDelete.exists()) {
                                        pw.println("ERROR_FILE_DOES_NOT_EXIST " + fileToRemove); // To Controller
                                    } else {

                                        while (!fileToDelete.delete()) {
                                        }
                                        dStore.files.remove(fileToRemove);
                                    }
                                }

                                pw.println("REBALANCE_COMPLETE");

                            }
                            // REBALANCE is aborted



                        }

                        default -> {
                            // LOG ME
                            System.out.println( "[" + this.getName() + " Controller]" +  "Message Received: " + line + " On Port: " + controllerSocket.getPort());
                        }

                        /*case "STORE" -> {
                            String fileName = lineSplit[1];
                            int fileSize = Integer.parseInt(lineSplit[2]);
                            // check it can store the file
                            pw.println("ACK");

                            File fileToWrite = new File(fileFolder + File.pathSeparator + fileName );

                            byte[] bytes = null;
                            while ( (bytes = in.readAllBytes()) != null){

                            }

                            // recive file
                            // store file
                            // pw.println("STORE_ACK " + fileName); (To Contoller)
                        }


                        case "LOAD_DATA" -> {
                            String fileName = lineSplit[1];

                            // if file to load doesnt exist close socket;

                            out.write(Files.readAllBytes(Path.of(fileFolder + File.pathSeparator + fileName)));
                        }*/
                    }

                }else{
                    //peer Closed Socket
                    System.out.println("[" + this.getName() + " Controller] " + "Client Closed Socket");
                    break;
                }
            } catch (IOException e) {
                // Connection to Controller is Persistent (TimeOut)
            }
        }

        System.out.println("[" + this.getName() + " Controller] Out of While True??");

        disconnect();

    }

    public void sendMessage( String message ){
        pw.println( message );
    }

    private void disconnect() {
        System.out.println( "[" + this.getName() + " Controller ]" +  "DISCONNECTING");

        try {
            if (controllerSocket != null) {
                //pw.println("disconnect");
                controllerSocket.close();
            }
        } catch (IOException ignored) {
        }

        if (pw != null) {
            pw.close();
        }

        try {
            if (br != null) {
                br.close();
            }
        } catch (IOException ignored) {
        }



        /*try {
            if (in != null) {
                in.close();
            }
        } catch (IOException ignored) {
        }

        try {
            if (out != null) {
                out.close();
            }
        } catch (IOException ignored) {
        }*/
    }




}
