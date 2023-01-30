import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


// Handles Client and Dstore connections
public class ControllerConnectionHandler extends Thread implements CommunicationReceivedListener {

    private Socket clientSocket;
    private PrintWriter pw = null;
    private BufferedReader br = null;
    private OutputStream connectionOut = null;
    private InputStream connectionIn = null;
    private Logger logger = Logger.getLogger(String.valueOf(ControllerConnectionHandler.class));
    private final Controller controller;
    private boolean isDstore = false;
    private int timeOut;

    private volatile ArrayList<Integer> waitingForStoreAckFromPorts = new ArrayList<>();
    private volatile String waitingForStoreAckFileName = "";

    private volatile ArrayList<Integer> waitingForRemoveAckFromPorts = new ArrayList<>();
    private volatile String waitingForRemoveAckFileName = "";
    private volatile CountDownLatch removeACKLatch;
    private volatile CountDownLatch storeACKLatch;

    public volatile boolean storeOrRemoveActive = false;


    private volatile Map<String, ArrayList<Integer>> loadAlreadyFailedFromDstores = new HashMap<>();

    private static int connectedClients = 0;


    private volatile ArrayList<String> messageQueue = new ArrayList<>();


    public ControllerConnectionHandler(Socket clientSocket, Controller controller, int timeout) {
        this.clientSocket = clientSocket;
        this.controller = controller; // Its not actually an error
        this.timeOut = timeout;
        controller.addNormListener(this);
    }

    @Override
    public void run(){
        System.out.println(clientSocket.getPort() + ": " + "Device Connected to Controller Handler From Port " + clientSocket.getPort());

        try {
            pw = new PrintWriter(clientSocket.getOutputStream(), true);
            br = new BufferedReader( new InputStreamReader( clientSocket.getInputStream() ) );

            connectionIn = clientSocket.getInputStream();
            connectionOut = clientSocket.getOutputStream();

        } catch (IOException e) {
            e.printStackTrace();
        }


        String[] lineSplit = null;
        while (true) {
            String lineRead = null;

            System.out.println(clientSocket.getPort() + ": Start While");
            try {

                if (messageQueue.size() > 0){
                    System.out.println(clientSocket.getPort() + ": Message Queue Start");
                    while (controller.rebalanceActive){
                    }

                    System.out.println(clientSocket.getPort() + ": Proeccessing Queued Message");

                    for ( String line : messageQueue) {
                        proccessMessage(line);
                    }
                    messageQueue.clear();
                }

                System.out.println(clientSocket.getPort() + ": Waiting for message from ReadLine");

                if ((lineRead = br.readLine()) != null ) {
                    System.out.println(clientSocket.getPort() + ": Line Read ROOT " + lineRead);

                    if(!controller.rebalanceActive || isDstore ) {
                        // Message Parsing
                        //System.out.println(clientSocket.getPort() + ": 333");

                        if (!messageQueue.contains(lineRead)){
                            System.out.println(clientSocket.getPort() + ": adding message to Queue for processing NOW: " + lineRead);

                            messageQueue.add(lineRead);
                        }else{
                            System.out.println(clientSocket.getPort() + ":Message allready in queue: " + lineRead );
                        }

                        System.out.println(clientSocket.getPort() + ": Message Queue is: " + messageQueue);


                        for ( String line : messageQueue) {

                            proccessMessage(line);

                        }
                        messageQueue.clear();

                    } else{
                        if (!messageQueue.contains(lineRead)){
                            messageQueue.add(lineRead);
                            System.out.println(clientSocket.getPort() + ": adding message to Queue for processing LATER: " + lineRead + " : " + messageQueue);
                        }else{
                            System.out.println(clientSocket.getPort() + ":Message allready in queue: " + lineRead + " : " + messageQueue);
                        }
                    }

                }else{
                    //peer Closed Socket
                    System.out.println(clientSocket.getPort() + ": " +"Client Closed Socket");

                    if (isDstore){
                        System.out.println(clientSocket.getPort() + ": DSTORE CRASHED DOING CLEAN UP");

                        cleanUpCrashedDStore();
                    }

                    break;
                }
            } catch (IOException e) {


                //e.printStackTrace(); //Read Timed connectionOut

                // Connection to Controller is Persistent
                if ( !isDstore ) {
                    //System.out.println(clientSocket.getPort() + ": " + "[CONTROLLER] Read Line ERROR" + " On Port: " + clientSocket.getPort());
                    System.out.println(clientSocket.getPort() + ": " + "Client Timeout: " + messageQueue);

                    /*if ( waitingForStoreAckFromPorts.size() != 0 ){
                        System.out.println(clientSocket.getPort() + ": " + clientSocket.getPort() + ": " + "STORE_ACK not received form all Dstores, REMOVING " + waitingForStoreAckFileName + " FROM INDEX");
                        controller.controllerIndex.remove( waitingForStoreAckFileName );
                    }*/

                    disconnect();
                    break;
                }

                /*System.conectionOut.println("SENDING REMOVE MESSAGE TO DSTORE");
                pw.println("REMOVE fake_file");*/

                /*System.out.println(clientSocket.getPort() + ": " + clientSocket.getPort() + ": " + "SENDING STORE MESSAGE TO DSTORE");
                pw.println("STORE file_to_store.txt 28");
                String resp = null;
                try {
                     resp = br.readLine();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
                System.out.println(clientSocket.getPort() + ": " + clientSocket.getPort() + ": " + "RESPONSE IS: " + resp);
                if (resp.equals("ACK")){
                    try {
                        String Spath = DstoreClientHandler.class.getClassLoader().getResource("client_to_store/file_to_store").getPath();
                        File file = new File(Spath);

                        byte[] bytes = Files.readAllBytes(  file.toPath()  );

                        connectionOut.write( bytes );
                        connectionOut.flush();

                        System.out.println(clientSocket.getPort() + ": " + clientSocket.getPort() + ": " + "FINISHED WRITING BYTES");
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                }*/
            } catch (Exception e) {
                e.printStackTrace();
            }
            //System.out.println(clientSocket.getPort() + ": END OF WHILE");
        }

        //System.conectionOut.println(clientSocket.isConnected());
        //System.conectionOut.println(clientSocket.isClosed());
        //System.out.println(clientSocket.getPort() + ": " + "Socket Closed" + " On Port: " + clientSocket.getPort());

        System.out.println(clientSocket.getPort() + ": Out Of While Ture -> DISCONECT");

        disconnect();
    }

    private void cleanUpCrashedDStore(){
        synchronized (controller){
            var dStore = controller.dstoreConnectionToPortLookUp.get(clientSocket.getPort());
            controller.dstoreFileIndex.remove(dStore);

            for ( String fileName : controller.fileStoredLocations.keySet()) {
                controller.fileStoredLocations.get(fileName).remove(dStore);

                if (controller.fileStoredLocations.get(fileName).size() == 0){
                    // remove file too
                    controller.controllerIndex.remove(fileName);
                    controller.fileSizes.remove(fileName);
                }
            }

            controller.connectedDstores.remove(clientSocket);

            controller.removeDstoreListeners(this);

            controller.dstoreConnectionToPortLookUp.remove(clientSocket.getPort());
        }

        System.out.println(clientSocket.getPort() + ": DSTORE CLEAN UP COMPLETE");

        System.out.println("------" + controller.dstoreFileIndex);
        System.out.println("------" + controller.fileStoredLocations);
        System.out.println("------" + controller.controllerIndex);
        System.out.println("------" + controller.fileSizes);
        System.out.println("------" + controller.connectedDstores);
        System.out.println("------" + controller.dstoreConnectionToPortLookUp);


    }

    private void proccessMessage(String message) {

        String[] lineSplit = null;
        String lineRead = null;


        lineSplit = message.split(" ");


        switch (lineSplit[0]) {

            case "JOIN" -> {
                System.out.println(clientSocket.getPort() + ": " + "DSTORE CONNECTED FROM PORT: " + clientSocket.getPort() + " Dstore Clients can connect on: " + lineSplit[1] + " Triggering Rebalance");

                isDstore = true;


                synchronized (controller) {
                    controller.connectedDstores.add(clientSocket);
                    controller.dstoreFileIndex.put(Integer.parseInt(lineSplit[1]), new HashMap<String, FileState>());
                    controller.dstoreConnectionToPortLookUp.put(clientSocket.getPort(), Integer.parseInt(lineSplit[1]));
                    controller.removeNormListeners(this);
                    controller.addDStoreListener(this);
                }

                System.out.println(clientSocket.getPort() + ": UPDATED CONNECTED DSTORES: " + controller.connectedDstores);
                System.out.println(clientSocket.getPort() + ": UPDATED DSTORE FILE INDEX: " + controller.dstoreFileIndex);





                //System.out.println(clientSocket.getPort() + ": rebal active? " + controller.rebalanceActive);
                if (!controller.rebalanceActive) {
                    controller.rebalanceActive = true;
                    //System.out.println(clientSocket.getPort() + ": Im Going In");

                    Runnable reBalanceHandler = new Runnable() {
                        @Override
                        public void run() {
                            //wait for stores to finish
                            var activeStoreOrRemove = true;
                            while (activeStoreOrRemove){
                                activeStoreOrRemove = false;
                                for ( ControllerConnectionHandler handler: controller.connectionHandlers) {
                                    if(handler.storeOrRemoveActive){
                                        activeStoreOrRemove = true;
                                    }
                                }
                                System.out.println("[REBALANCE] Waiting for store or Remove to finish");
                            }

                            controller.triggerRebalance();
                        }
                    };

                    Thread reBalanceThread = new Thread(reBalanceHandler);
                    reBalanceThread.start();

                }

                //System.out.println(clientSocket.getPort() + ": Finished with Controller Lock: " + controller.rebalanceActive);
                //}

                            /*try {
                                for (int i = 0; i < 10; i++) {
                                    System.out.println(clientSocket.getPort() + ": " + "1: " + controller.connectedDstores.get(i).getPort());
                                    System.out.println(clientSocket.getPort() + ": " + "2: " + controller.dstoreConnectionToPortLookUp.get( controller.connectedDstores.get(i).getPort() ));
                                }
                            }catch (Exception e){ }*/
                //System.out.println(clientSocket.getPort() + ": Finished JOIN");
                break;
            }

            case "STORE_ACK" -> {
                if (isDstore) {
                    var fileName = lineSplit[1];
                    System.out.println(clientSocket.getPort() + ": " + "STORE_ACK RECEIVED FOR " + fileName + " ON PORT: " + controller.dstoreConnectionToPortLookUp.get(clientSocket.getPort()));
                    controller.triggerNormListeners("STORE_ACK " + fileName, controller.dstoreConnectionToPortLookUp.get(clientSocket.getPort()));

                    //Map<String, Set<Integer>> fileStoredLocations = new HashMap<>();

                                    /*synchronized (controller) {
                                        if (controller.fileStoredLocations.containsKey(fileName)) {
                                            controller.fileStoredLocations.get(fileName).add(controller.dstoreConnectionToPortLookUp.get(clientSocket.getPort()));
                                            System.out.println(clientSocket.getPort() + ": " + "UPDATED fileStoredLocations: " + controller.fileStoredLocations.toString());
                                        } else {
                                            controller.fileStoredLocations.put(fileName, new ArrayList<Integer>());
                                            controller.fileStoredLocations.get(fileName).add(controller.dstoreConnectionToPortLookUp.get(clientSocket.getPort()));

                                            System.out.println(clientSocket.getPort() + ": " + "INIT fileStoredLocations: " + controller.fileStoredLocations.toString());
                                        }
                                    }*/


                    // removing this port will be done via the listener
                    //waitingForStoreAckFromPorts.remove(Integer.parseInt(fileName));
                }
            }

            case "STORE" -> {

                storeOrRemoveActive = true;

                String fileName = lineSplit[1];
                int fileSize = Integer.parseInt(lineSplit[2]);

                System.out.println(clientSocket.getPort() + ": " + "STARTING STORE COMMAND: " + fileName);



                synchronized (controller) {
                    // Not enough DStores have joined
                    if (controller.dstoreFileIndex.size() < controller.rNum) {
                        pw.println("ERROR_NOT_ENOUGH_DSTORES");
                        System.out.println(clientSocket.getPort() + ": STORE " + "ERROR_NOT_ENOUGH_DSTORES");
                        break;
                    }
                    // File allready exists conectionIn the Storage Network
                    else if (controller.controllerIndex.get(fileName) != null) {
                        pw.println("ERROR_FILE_ALREADY_EXISTS");
                        System.out.println(clientSocket.getPort() + ": " + "ERROR_FILE_ALREADY_EXISTS");
                        break;
                    }

                    //Update Controller Index
                    controller.controllerIndex.put(fileName, FileState.STORING);
                }

                System.out.println(clientSocket.getPort() + ": UPDATED controllerIndex IN STORE COMMAND: " + fileName);



                // get Dstore Ports to store to
                ArrayList<Integer> intPortsToStoreTo = controller.getDstoresToStoreTo(fileName, fileSize);
                StringBuilder stringPortsToStoreTo = new StringBuilder();

                                    /*if (waitingForStoreAckFromPorts.size() != 0) {
                                        throw new Exception("ERROR");
                                    }*/

                waitingForStoreAckFromPorts.clear();

                for (Integer port : intPortsToStoreTo) {
                    stringPortsToStoreTo.append(port).append(" ");
                    waitingForStoreAckFromPorts.add(port);
                }

                System.out.println(clientSocket.getPort() + ": RETURNING LIST OF PORTS TO STORE TO: " + intPortsToStoreTo);

                waitingForStoreAckFileName = fileName;

                storeACKLatch = new CountDownLatch(waitingForStoreAckFromPorts.size());

                while (storeACKLatch == null){}



                // respond to Client
                pw.println("STORE_TO " + stringPortsToStoreTo.toString().trim());


                try {
                    if (storeACKLatch.await(timeOut, TimeUnit.MILLISECONDS)) {

                        System.out.println(clientSocket.getPort() + ": " + "WAITING FOR ACK OVER");

                        // Received "STORE_ACK " + fileName on all given ports
                        //update indexes
                        synchronized (controller) {
                            controller.controllerIndex.put(fileName, FileState.STORED);
                            controller.fileSizes.put(fileName, fileSize);

                            for (Integer port : intPortsToStoreTo) {

                                if (!controller.dstoreFileIndex.containsKey(port)) {
                                    controller.dstoreFileIndex.put(port, new HashMap<>());
                                }
                                controller.dstoreFileIndex.get(port).put(fileName, FileState.STORED);


                                if (!controller.fileStoredLocations.containsKey(fileName)) {
                                    controller.fileStoredLocations.put(fileName, new ArrayList<Integer>());

                                }

                                if (!controller.fileStoredLocations.get(fileName).contains(port)){
                                    controller.fileStoredLocations.get(fileName).add(port);
                                }


                            }
                        }
                        System.out.println(clientSocket.getPort() + ": " + "UPDATED fileStoredLocations: " + controller.fileStoredLocations.toString());
                        System.out.println(clientSocket.getPort() + ": " + "STORE COMPLETE: " + fileName + " controllerIndex: " + controller.controllerIndex.toString() + " : " + controller.dstoreFileIndex);


                        pw.println("STORE_COMPLETE");
                    }
                    else {
                        //timeOut
                        System.out.println(clientSocket.getPort() + ": Waiting For Store Ack Timmed Out");
                        controller.controllerIndex.remove(fileName);
                    }
                }catch (InterruptedException e){
                    //interrupted
                    System.out.println("SDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDSSSSSSSSSSSSSSSSDSDSD");
                    controller.controllerIndex.remove(fileName);
                }

                storeOrRemoveActive = false;
            }

            case "LOAD" -> {
                String fileName = lineSplit[1];

                System.out.println(clientSocket.getPort() + ": " + "STARTING LOAD COMMAND: " + fileName);

                // Not enough DStores have joined
                if (controller.dstoreFileIndex.size() < controller.rNum) {
                    pw.println("ERROR_NOT_ENOUGH_DSTORES");
                    System.out.println(clientSocket.getPort() + ": LOAD " + "ERROR_NOT_ENOUGH_DSTORES");
                    break;
                }
                // File doesn't exists conectionIn the Storage Network
                else if ((controller.controllerIndex.get(fileName) == null) || (controller.controllerIndex.get(fileName) != FileState.STORED)) {
                    pw.println("ERROR_FILE_DOES_NOT_EXIST");
                    System.out.println(clientSocket.getPort() + ": " + "ERROR_FILE_DOES_NOT_EXIST");
                    break;
                }


                                    /*try {
                                        System.out.println(clientSocket.getPort() + ": " + "LOADED FROM ALREADY: " + loadAlreadyFailedFromDstores.get(fileName).toString());
                                    } catch (Exception e) {
                                        System.out.println(clientSocket.getPort() + ": " + "LOADED FROM ALREADY: []");
                                    }*/

                //get DStore to LOAD from
                int port = -1;
                if (loadAlreadyFailedFromDstores.containsKey(fileName)) {
                    loadAlreadyFailedFromDstores.get(fileName).clear();
                } else {
                    loadAlreadyFailedFromDstores.put(fileName, new ArrayList<>());
                }
                port = controller.getDstoreToLoadFrom(loadAlreadyFailedFromDstores.get(fileName), fileName);



                //get file size
                int fileSize = controller.fileSizes.get(fileName);

                pw.println("LOAD_FROM " + port + " " + fileSize);

                System.out.println(clientSocket.getPort() + ": " + "LOAD FROM RESPONSE: " + port);

                loadAlreadyFailedFromDstores.get(fileName).add(port);
            }

            case "RELOAD" -> {
                String fileName = lineSplit[1];

                System.out.println(clientSocket.getPort() + ": " + "RELOAD COMMAND: " + fileName);


                // Not enough DStores have joined
                if (controller.dstoreFileIndex.size() < controller.rNum) {
                    pw.println("ERROR_NOT_ENOUGH_DSTORES");
                    System.out.println(clientSocket.getPort() + ": RELOAD " + "ERROR_NOT_ENOUGH_DSTORES");

                    break;
                }
                // File doesn't exists conectionIn the Storage Network
                else if (controller.controllerIndex.get(fileName) == null) {
                    pw.println("ERROR_FILE_DOES_NOT_EXIST");
                    System.out.println(clientSocket.getPort() + ": " + "ERROR_FILE_DOES_NOT_EXIST");

                    break;
                }


                try {
                    System.out.println(clientSocket.getPort() + ": " + "LOADED FROM ALREADY: " + loadAlreadyFailedFromDstores.get(fileName).toString());
                } catch (Exception e) {
                    System.out.println(clientSocket.getPort() + ": " + "LOADED FROM ALREADY: []");
                }


                //get DStore to LOAD from
                int port = -1;
                if (!loadAlreadyFailedFromDstores.containsKey(fileName)) {
                    loadAlreadyFailedFromDstores.put(fileName, new ArrayList<>());
                }
                port = controller.getDstoreToLoadFrom(loadAlreadyFailedFromDstores.get(fileName), fileName);

                if (port == -1) {
                    pw.println("ERROR_LOAD");
                    System.out.println(clientSocket.getPort() + ": ERROR LOAD");
                    loadAlreadyFailedFromDstores.get(fileName).clear();
                    break;
                }

                //get file size
                int fileSize = controller.fileSizes.get(fileName);

                pw.println("LOAD_FROM " + port + " " + fileSize);
                System.out.println(clientSocket.getPort() + ": LOAD_FROM SEND: " + port);
                loadAlreadyFailedFromDstores.get(fileName).add(port);
            }

            case "REMOVE" -> {

                storeOrRemoveActive = true;

                String fileName = lineSplit[1];

                System.out.println(clientSocket.getPort() + ": REMOVE COMMAND RECEIVED: " + fileName);

                System.out.println(clientSocket.getPort() + ": " + controller.controllerIndex.toString());

                // Not enough DStores have joined
                if (controller.dstoreFileIndex.size() < controller.rNum) {
                    pw.println("ERROR_NOT_ENOUGH_DSTORES");
                    break;
                }
                // File doesn't exists conectionIn the Storage Network
                else if ((controller.controllerIndex.get(fileName) == null) || (controller.controllerIndex.get(fileName) != FileState.STORED)) {
                    pw.println("ERROR_FILE_DOES_NOT_EXIST");
                    System.out.println(clientSocket.getPort() + ": TRIED TO REMOVE " + fileName + " DOES NOT EXSIST IN INDEX: " + controller.controllerIndex.toString());
                    break;
                }

                controller.controllerIndex.put(fileName, FileState.REMOVING);
                System.out.println(clientSocket.getPort() + ": " + controller.controllerIndex.toString());


                waitingForRemoveAckFromPorts.clear();

                // add all dstore with this file to waiting list for REMOVE_ACK
                var fileStoredLocations = controller.fileStoredLocations.get(fileName);
                waitingForRemoveAckFromPorts.addAll(fileStoredLocations);
                waitingForRemoveAckFileName = fileName;

                System.out.println(clientSocket.getPort() + ": INIT Waiting for acks: " + waitingForRemoveAckFromPorts + " : " +  waitingForRemoveAckFromPorts.size());

                System.out.println(clientSocket.getPort() + ": Sending REMOVE message to Dstores: " + fileStoredLocations);

                // remove from Controller before removing from DStores
                synchronized (controller) {
                    controller.controllerIndex.remove(fileName);
                    controller.fileStoredLocations.remove(fileName);
                    controller.fileSizes.remove(fileName);
                    for ( Integer dStore: controller.dstoreFileIndex.keySet()) {
                        controller.dstoreFileIndex.get( dStore ).remove(fileName);
                    }
                }

                removeACKLatch = new CountDownLatch(fileStoredLocations.size());

                while (removeACKLatch == null){}

                for (int dStorePort : fileStoredLocations) {
                    // send message "REMOVE " + fileName
                    System.out.println(clientSocket.getPort() + ": Sent REMOVE  to dStore: " + dStorePort);
                    controller.triggerDstoreListeners("REMOVE " + fileName, dStorePort);
                }


                System.out.println(clientSocket.getPort() + ": Sent all Remove Commands To Dstores ");



                try {
                    if (!removeACKLatch.await(timeOut, TimeUnit.MILLISECONDS)) {
                        System.out.println(clientSocket.getPort() + ": " + "WAITING FOR ACK TIMEOUT");
                    } else {
                        System.out.println(clientSocket.getPort() + ": " + "WAITING FOR ACK OVER");
                    }
                } catch (InterruptedException e) {
                    System.out.println(clientSocket.getPort() + ": WAITING FOR ACK InterRuption");

                    e.printStackTrace();
                }


                System.out.println(clientSocket.getPort() + ": " + "REMOVE COMPLETE: " + controller.dstoreFileIndex + controller.controllerIndex + controller.fileStoredLocations + controller.fileSizes);
                pw.println("REMOVE_COMPLETE");

                storeOrRemoveActive = false;

            }

            case "LIST" -> {

                System.out.println(clientSocket.getPort() + ": ROOT LIST RECEIVED:");

                // List operation originates from a Client request for a LIST
                if (lineSplit.length == 1 && !isDstore) {

                    System.out.println(clientSocket.getPort() + ": " + "LIST COMMAND RECEIVED");

                    // Not enough DStores have joined
                    if (controller.dstoreFileIndex.size() < controller.rNum) {
                        pw.println("ERROR_NOT_ENOUGH_DSTORES");
                        System.out.println(clientSocket.getPort() + ": LIST ERROR_NOT_ENOUGH_DSTORES " + controller.dstoreFileIndex.size() + " : " + controller.rNum);
                        break;
                    }

                    // include REMOVING files?
                    StringBuilder fileNames = new StringBuilder();
                    synchronized (controller) {
                        for (String fileName : controller.controllerIndex.keySet()) {
                            if (controller.controllerIndex.get(fileName) == FileState.STORED) {
                                fileNames.append(fileName).append(" ");
                            }
                        }
                    }
                    pw.println("LIST " + fileNames.toString().trim());
                    System.out.println(clientSocket.getPort() + ": " + "LIST COMPLETED: " + fileNames.toString().trim() + " : " + controller.controllerIndex);

                } else if (isDstore) {
                    // is a LIST response message

                    System.out.println(clientSocket.getPort() + ": " + "LIST Response Receveied: " + message);

                    // List Response form Dstore
                    ArrayList<String> parsedFileNamesArray;

                    if (lineSplit.length == 1) {
                        parsedFileNamesArray = new ArrayList<String>();
                    } else {
                        parsedFileNamesArray = new ArrayList<String>(Arrays.asList(lineSplit[1].split(" ")));
                    }
                    controller.rebalanceAudits.put(controller.dstoreConnectionToPortLookUp.get(clientSocket.getPort()), parsedFileNamesArray);

                    //System.out.println(clientSocket.getPort() + ": " + "LIST Response Recorded: " + controller.rebalanceAudits);
                    // HERE
                    controller.rebalanceListLatch.countDown();

                } else {
                    System.out.println(clientSocket.getPort() + ": ERRRRRRRRORRRRRRRRR");
                }
            }

            case "REMOVE_ACK" -> {
                if (isDstore) {
                    System.out.println(clientSocket.getPort() + ": REMOVE ACK RECEIVED DIRECT FROM DSTORE");
                    String fileName = lineSplit[1];

                    var dStorePort = controller.dstoreConnectionToPortLookUp.get(clientSocket.getPort());

                    //remove from Dstore Index
                    //controller.dstoreFileIndex.get(dStorePort).remove(fileName); // good

                    // Remove Dstore form fileStoredLocations
                                    /*System.out.println(clientSocket.getPort() + ": " + "Before IFFY: " + controller.fileStoredLocations);
                                    controller.fileStoredLocations.get(fileName).remove((Integer) dStorePort); // iffy
                                    System.out.println(clientSocket.getPort() + ": " + "After IFFY: " + controller.fileStoredLocations);*/
                    controller.triggerNormListeners("REMOVE_ACK " + fileName, dStorePort);
                }
            }

            case "REBALANCE_COMPLETE" -> {
                if (isDstore) {
                    System.out.println(clientSocket.getPort() + ": REBALANCE_COMPLETE RECEVIED");

                    controller.rebalanceLatch.countDown();
                }
            }

            case "ERROR_FILE_DOES_NOT_EXIST" -> {
            }

            default -> {
                // LOG ME
                System.out.println(clientSocket.getPort() + ": " + "UNKNOWN Message Received: " + message + " On Port: " + clientSocket.getPort());
            }
        }
    }

    private void disconnect(){
        System.out.println( clientSocket.getPort() + ": DISCONNECTING");

        try {
            if (clientSocket != null) {
                //pw.println("disconnect");
                clientSocket.close();
            }
        } catch (IOException e) { e.printStackTrace();}

        if ( pw != null){
            pw.close();
        }

        try {
            if ( br != null) {
                br.close();
            }
        } catch (IOException ignored) { }

        try {
            if ( connectionIn != null) {
                connectionIn.close();
            }
        } catch (IOException ignored) { }

        try {
            if (connectionOut != null) {
                connectionOut.close();
            }
        } catch (IOException ignored) { }

    }


    @Override
    public synchronized void receivedMessage(String message, int dStorePort) {

        String[] lineSplit = message.split(" ");
        String command = lineSplit[0];

        System.out.println(clientSocket.getPort() + ": Received Message Over Listener: " + message + " From: " + dStorePort + " : " + controller.dstoreConnectionToPortLookUp);

        if (command.equals("REMOVE_ACK")) {
            //System.out.println( clientSocket.getPort() + ": Before Remove waiting for Port: " + waitingForRemoveAckFromPorts );
            //waitingForRemoveAckFromPorts.remove((Integer) dStorePort);
            if (waitingForRemoveAckFromPorts.contains(dStorePort)) {
                System.out.println(clientSocket.getPort() + ": REMOVE ACK FOR THIS THREAD -> count Down Latch");
                removeACKLatch.countDown();
            }
            //System.out.println( clientSocket.getPort() + ": After Remove waiting for Port: " + waitingForRemoveAckFromPorts );

        }
        else if (command.equals("STORE_ACK")) {
            //System.out.println(clientSocket.getPort() + ": " + "STORE ACK Received by listener for : " + lineSplit[1] + " From: " + dStorePort);
            //waitingForStoreAckFromPorts.remove((Integer) dStorePort);
            var fileName = lineSplit[1];
            if (waitingForStoreAckFromPorts.contains(dStorePort) && fileName.equals(waitingForStoreAckFileName)) {
                waitingForStoreAckFromPorts.remove( (Integer) dStorePort);
                System.out.println(clientSocket.getPort() + ": STORE ACK FOR THIS THREAD -> count Down Latch");
                storeACKLatch.countDown();
                System.out.println(clientSocket.getPort() + ": Counting down Latch: " + message + " : " + dStorePort + " : " + storeACKLatch.getCount() + "Remaining");

            }

            //System.out.println(clientSocket.getPort() + ": " + "New waiting for acks: " + waitingForStoreAckFromPorts.toString());

        }
        else if (isDstore && command.equals("REMOVE") && (dStorePort == controller.dstoreConnectionToPortLookUp.get(clientSocket.getPort()))) {
            String fileName = lineSplit[1];
            System.out.println(clientSocket.getPort() + ": " + "Forward REMOVE COMMAND Received: " + fileName + " " + dStorePort);
            pw.println("REMOVE " + fileName);
        }
        else if (isDstore && command.equals("LIST") && dStorePort == -1 ){
            // send list command to DStore
            System.out.println(clientSocket.getPort() + ": sendinggg LIST command to Dstore" );
            pw.println("LIST");
        }
        else if (isDstore && command.equals("REBALANCE") && (dStorePort == controller.dstoreConnectionToPortLookUp.get(clientSocket.getPort()))){
            System.out.println(clientSocket.getPort() + ": " + "Forward REBALANCE COMMAND Received: " + message + dStorePort);
            pw.println(message);
        }


    }

}
