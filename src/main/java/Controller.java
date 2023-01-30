
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Controller {

    // If connection with cw2207.Dstore drops remove it from the set of Dstores

    // ALL DSTORE PORT REFERENCES IS THE PORT DSTORES ARE ACEPTING ON NOT OUTGOING TO CONTORLLER (Use dstoreConnectionToPortLookUp to convert)

    /*
        DStore Port, FileName, FileState
        Dstore added on CONNECTION
        Dstore Removed on DISCONNECT or timeout

        Files added on STORE completion
        Files removed on REMOVE_ACK
    */
    protected volatile ConcurrentHashMap<Integer, Map<String, FileState>> dstoreFileIndex = new ConcurrentHashMap<>();

    /*
        File to Dstore index
        Files added on STORE_ACK
        Files removed on REMOVE_ACK
     */
    protected volatile ConcurrentHashMap<String, ArrayList<Integer>> fileStoredLocations = new ConcurrentHashMap<>();

    /*
      All files state
      Files added on start of STORE
      Files removed on REMOVE_COMPLETE or failed STORE
    */
    protected volatile ConcurrentHashMap<String, FileState> controllerIndex = new ConcurrentHashMap<>();

    /*
        all Files size
        Files added on STORE COMPLETION
        Files removed on REMOVE_COMPLETE
    */
    protected volatile ConcurrentHashMap<String, Integer> fileSizes = new ConcurrentHashMap<>();

    /*
        all connected dStore sockets
        Dstore added on CONNECTION
        Dstore Removed on DISCONNECT or timeout
    */
    protected volatile List<Socket> connectedDstores = Collections.synchronizedList(new ArrayList<>());

    /*
        keys are outgoing ports from Dstore to the Controller, Values is port Dstores are accepting clients on
        Dstore added on CONNECTION
        Dstore Removed on DISCONNECT or timeout
    */
    protected volatile ConcurrentHashMap<Integer,Integer> dstoreConnectionToPortLookUp = new ConcurrentHashMap<>();

    // The List of fimeNames as well as teh dstore they belong to
    protected volatile ConcurrentHashMap<Integer, ArrayList<String>> rebalanceAudits = new ConcurrentHashMap<>();



    private Logger logger = Logger.getLogger(String.valueOf(Controller.class));
    private ServerSocket serverSocket = null;
    private PrintWriter pw = null;
    private BufferedReader br = null;
    protected int controllerPort;
    protected int rNum;
    protected int timeout;
    protected volatile int rebalancePeriod;
    protected volatile boolean rebalanceActive;

    private Object obj = new Object();

    private final List<CommunicationReceivedListener> listenersForAllConnectedHandlers = Collections.synchronizedList(new ArrayList<>());
    private final List<CommunicationReceivedListener> listenersForAllConnectedDStores = Collections.synchronizedList(new ArrayList<>());

    //private RebalanceTimer rebalanceTimer;


    private final HashMap<String, Integer> fileOverStoredBy = new HashMap<String, Integer>();
    private final HashMap<String, Integer> fileUnderStoredBy = new HashMap<String, Integer>();

    //we need to add Files from dStore
    private final HashMap<Integer, Integer> dStoresAdjustmentFactorPos = new HashMap<>();
    // we need to remove files from dStore
    private final HashMap<Integer, Integer> dStoresAdjustmentFactorNeg = new HashMap<>();

    // how many files can we add or remove untill we are on teh boundary (cant add or remove 1 more)
    private final HashMap<Integer, Integer> dStoresFreeCapacityToUpperBound = new HashMap<>();
    private final HashMap<Integer, Integer> dStoresFreeCapacityToLowerBound = new HashMap<>();


    private final HashMap<Integer, ArrayList<String>> toRemove = new HashMap<>();
    private final HashMap<String, ArrayList<Integer>> toAdd = new HashMap<>();

    protected volatile CountDownLatch rebalanceLatch;
    protected volatile CountDownLatch rebalanceListLatch;


    protected ArrayList<ControllerConnectionHandler> connectionHandlers = new ArrayList<>();


    //protected volatile Boolean triggerRebalanceFlag = false;



    public Controller(int controllerPort, int rNum, int timeout, int rebalancePeriod){
        this.controllerPort = controllerPort;
        this.rNum = rNum;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;
        this.rebalanceActive = false;
        startServer();
    }

    public static void main(String[] args) {

        if ( args.length == 4) {
            int controllerPort = Integer.parseInt(args[0]);
            int rNum = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            int rebalancePeriod = Integer.parseInt(args[3]);

            Controller controller = new Controller(controllerPort, rNum, timeout, rebalancePeriod);
        }/*else{
            Controller controller = new Controller(12345, 2, 10000, 1000);
        }*/
    }

    private void startServer(){
        // Create Server Socket
        try {
            serverSocket = new ServerSocket(controllerPort);
            //serverSocket.setSoTimeout(timeout);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //start Rebalence Timer
        //rebalanceTimer = new RebalanceTimer( this, rebalancePeriod);
        //rebalanceTimer.start();


        // Listen for connections
        while (true) {
            try {
                //System.out.println("Looking for Connection");
                var handler = new ControllerConnectionHandler(serverSocket.accept(), this, timeout);
                connectionHandlers.add(handler);
                handler.start();

            } catch (IOException ignored) {}
        }

    }

    protected synchronized int getDstoreToLoadFrom( ArrayList<Integer> excluding, String fileName){
        // consecutive calls for same file should return different ports for DStores

        ArrayList<Integer> dStoresWhereFileStored = new ArrayList<>( fileStoredLocations.get(fileName) );


       System.out.println("[CONTROLLER]: fileStoredLocations:" + fileStoredLocations.toString());
       System.out.println("[CONTROLLER]: available Dstores: " + dStoresWhereFileStored + " To Exclude: " + excluding.toString());

        for ( Integer toExclude: excluding) {
            dStoresWhereFileStored.remove( toExclude );
        }

        if ( dStoresWhereFileStored.size() == 0 ){
            return -1;
        }else{
            return dStoresWhereFileStored.get(0);
        }

    }

    protected synchronized ArrayList<Integer> getDstoresToStoreTo(String fileName, int fileSize){
        var ports = new ArrayList<Integer>();

        int upperBoundStoreLimit = (int) Math.ceil( (float) (rNum * (fileStoredLocations.size() + 1)) / connectedDstores.size());


        //.out.println("UPPER BOUND IS: " + upperBoundStoreLimit);
        for ( Integer dStore: dstoreFileIndex.keySet()) {
            if (ports.size() == rNum){
                break;

            }else {
                //System.out.println("dStoreFileIndex Size: " + dstoreFileIndex.get(dStore).size());
                if (upperBoundStoreLimit - (dstoreFileIndex.get(dStore).size()) >= 1) {
                    ports.add(dStore);
                }
            }
        }

        /*for (int i = 0; i < rNum; i++) {
            ports[i] = dstoreConnectionToPortLookUp.get( connectedDstores.get(i).getPort() );
        }*/


        return ports;
    }

    public synchronized void addNormListener( CommunicationReceivedListener listener ){
        listenersForAllConnectedHandlers.add(listener);
    }

    protected synchronized void removeNormListeners(CommunicationReceivedListener listener) {
        listenersForAllConnectedHandlers.remove(listener);
    }

    public synchronized void addDStoreListener( CommunicationReceivedListener listener ) {
        System.out.println("[CONTROLLER] Added DStore To DStore Listener ");
        listenersForAllConnectedDStores.add(listener);
        System.out.println("[CONTROLLER] DStore Listener Array AFTER : " + listenersForAllConnectedDStores.toString() + " Size of: " + listenersForAllConnectedDStores.size());

    }

    protected synchronized void removeDstoreListeners(CommunicationReceivedListener listener) {
        listenersForAllConnectedDStores.remove(listener);
    }


    public synchronized void DStoreInfo(){
       System.out.println("[CONTROLLER] DStore Listener Array: " + listenersForAllConnectedDStores.toString() + " Size of: " + listenersForAllConnectedDStores.size());
    }

    protected synchronized void triggerNormListeners(String message, int dStorePort) {
        //System.out.println("TRIGGERED listenersForAllConnectedHandlers: " + listenersForAllConnectedHandlers.toString());
        for ( CommunicationReceivedListener listener: listenersForAllConnectedHandlers ) {
            listener.receivedMessage(message, dStorePort);
        }
    }

    protected synchronized void triggerDstoreListeners(String message, int dStorePort) {
        System.out.println("[CONTROLLER] TRIGGERED listenersForAllConnectedDStores: " + listenersForAllConnectedDStores.toString() + " Size of: " + listenersForAllConnectedDStores.size());

        for ( CommunicationReceivedListener listener: listenersForAllConnectedDStores ) {
            listener.receivedMessage(message, dStorePort);
        }
    }


    private void addToToRemove(Integer dStore, String fileName){
        if (toRemove.containsKey(dStore)){
            toRemove.get(dStore).add(fileName);
        }else{
            toRemove.put(dStore, new ArrayList<>(Collections.singletonList(fileName)));
        }
    }

    private void addToToAdd(Integer dStore, String fileName){

        if (toAdd.containsKey(fileName)){
            toAdd.get(fileName).add(dStore);
        }else{
            toAdd.put(fileName, new ArrayList<Integer>(Collections.singletonList(dStore)));
        }
    }

    private <S> void decrementMapKey(Map<S, Integer> map, S key){
        if (map.containsKey(key)) {
            if (map.get(key) > 1) {
                map.put(key, (map.get(key) - 1));
            } else {
                map.remove(key);
            }
        }
    }

    private <S> void incrementMapKey(Map<S, Integer> map, S key){
        if (map.containsKey(key)) {
            map.put(key, (map.get(key) + 1));
        }else{
            map.put(key, 1);
        }
    }


    protected synchronized void triggerRebalance() {

        rebalanceActive = true;



        if (connectedDstores.size() >= rNum) {
            if (controllerIndex.size() > 0) {

               System.out.println("[CONTROLLER] ReBalance Starting");

                // Rebalance SetUp

                // reset rebalanceTimer
                //rebalanceTimer.interrupt();

                //clear rebalanceAudits
                rebalanceAudits.clear();

                 /*for ( Socket dStore: connectedDstores) {
            triggerDstoreListeners( "LIST", dstoreConnectionToPortLookUp.get(dStore.getPort()));
        }*/

                if (listenersForAllConnectedDStores.size() != connectedDstores.size()) {
                    try {
                        throw new Exception("ERROR2");
                    } catch (Exception e) {
                        e.printStackTrace();

                    }

                    //System.exit(1);
                }

                var numOfDstores = listenersForAllConnectedDStores.size();

                // -1 for Broadcast
                //System.out.println("[CONTROLLER] Broadcasting LIST command for all connected Dstores: " + listenersForAllConnectedDStores + " " + numOfDstores);

                rebalanceListLatch = new CountDownLatch(numOfDstores);

                while (rebalanceListLatch == null) {
                }

                triggerDstoreListeners("LIST", -1);


                try {
                   System.out.println("[CONTROLLER] START waiting for LIST responses: " + rebalanceAudits.size() + System.currentTimeMillis());
                    rebalanceListLatch.await(timeout, TimeUnit.MILLISECONDS);


                } catch (InterruptedException e) {
                    //e.printStackTrace();
                }

                 /*var timeoutComplete = false;
            while ((rebalanceAudits.size() != numOfDstores)) { // What if a Dstore Crashes???
                if (!timeoutComplete) {
                    try {
                        wait(timeout);
                        timeoutComplete = true;
                    } catch (InterruptedException ignored) {
                    }
                } else {
                    break;
                }
            }

            if (timeoutComplete){
               System.out.println("[CONTROLLER] timedout waiting for LIST responses: " + rebalanceAudits.size());
            }else {
               System.out.println("[CONTROLLER] finished waiting for LIST responses: " + rebalanceAudits.size());
            }*/

               System.out.println("[CONTROLLER] finished waiting for LIST responses: " + rebalanceAudits);


                if (rebalanceAudits.size() != numOfDstores) {
                    // Dstore has crashed
                    //removeDstore()
                }


                toRemove.clear();
                toAdd.clear();

               System.out.println("------" + dstoreFileIndex);
               System.out.println("------" + fileStoredLocations);
               System.out.println("------" + controllerIndex);
               System.out.println("------" + fileSizes);
               System.out.println("------" + connectedDstores);
               System.out.println("------" + dstoreConnectionToPortLookUp);
               System.out.println("------" + rebalanceAudits);


                filterRemovedFiles();


                updateControllerWithDStoreAudits(rebalanceAudits);


                // Sync Audits to controller


                // Do Rebalance


                //Is each file Replicated R times over the Dstores??
                // Updates
                //  fileUnderStoredBy
                //  fileOverStoredBy
                checkFilesReplicatedRTimes();


                //Are FilesEvenly Distributed among the dStores?? (With F files, each Dstore should store between floor(RF/N) and ceil(RF/N) Files)
                // Updates
                //   dStoresAdjustmentFactorPos
                //   dStoresAdjustmentFactorNeg
                //   dStoresFreeCapacityToUpperBound
                //   dStoresFreeCapacityToLowerBound
                CheckFilesEvenlyDistributedAmongDStores();


                // BALANCE PHASE

                //System.out.println("\n\nBalancing Phase");


                while (dStoresAdjustmentFactorPos.size() != 0 || dStoresAdjustmentFactorNeg.size() != 0 || fileUnderStoredBy.size() != 0 || fileOverStoredBy.size() != 0) {
                /*System.out.println("\n\nNew While Cycle");
               System.out.println("DStores Adjustment Neg: " + dStoresAdjustmentFactorNeg);
               System.out.println("DStores Adjustment Pos: " + dStoresAdjustmentFactorPos);
               System.out.println("DStores Free Capacity to Upper: " + dStoresFreeCapacityToUpperBound);
               System.out.println("DStores Free Capacity to Lower: " + dStoresFreeCapacityToLowerBound);
               System.out.println("Files Over Stored: " + fileOverStoredBy);
               System.out.println("Files Under Stored: " + fileUnderStoredBy);
               System.out.println("To Add: " + toAdd);
               System.out.println("To Remove: " + toRemove);*/


                    //OS &  OL
                    //check if dstore is overLoaded and has a file which is over stored
                    // Updates
                    //   toRemove
                    //   dStoresAdjustmentFactorNeg
                    //   fileOverStoredBy
                    var overStoredAndOverLoadedFound = caseOverStoredAndOverLoaded();
                   System.out.println("\n\noverStoredAndOverLoadedFound: " + overStoredAndOverLoadedFound);


                    //OS
                    //check if dstore has an over stored file and removing it wont go over the lowerBound threshold
                    // Updates
                    //   toRemove
                    //   dStoresFreeCapacityToLowerBound
                    //   fileOverStoredBy
                    var overStoredAndAboveLowerBoundThreshHoldLoadedFound = false;
                    if (!overStoredAndOverLoadedFound) {
                        overStoredAndAboveLowerBoundThreshHoldLoadedFound = caseOverStored();
                       System.out.println("overStoredAndAboveLowerBoundThreshHoldLoadedFound: " + overStoredAndAboveLowerBoundThreshHoldLoadedFound);
                    }


                    //US  &  UL
                    //check if an under stored file can be added to a under loaded dstore
                    // Updates
                    //   toAdd
                    //   dStoresAdjustmentFactorPos
                    //   fileUnderStoredBy
                    var underStoredAndUnderLoadedFound = caseUnderStoredAndUnderLoaded();
                   System.out.println("underStoredAndUnderLoadedFound: " + underStoredAndUnderLoadedFound);


                    //US
                    //check if an under stored file can be added to a dstore with out going over the upper bound threshold
                    // Updates
                    //   toAdd
                    //   dStoresFreeCapacityToUpperBound
                    //   fileUnderStoredBy
                    var underStoredAndBelowUpperBoundThreshHoldLoadedFound = false;
                    if (!underStoredAndUnderLoadedFound) {
                        underStoredAndBelowUpperBoundThreshHoldLoadedFound = caseUnderStored();
                       System.out.println("underStoredAndBelowUpperBoundThreshHoldLoadedFound: " + underStoredAndBelowUpperBoundThreshHoldLoadedFound);

                    }

                    // if file still under stored ERRORRR
                    // do we need this???


                    //UL  & OL
                    //check if a pair of Over loaded and a Under Loaded Dstores exsist
                    // Updates
                    //   toAdd
                    //   dStoresAdjustmentFactorPos
                    //   toRemove
                    //   dStoresAdjustmentFactorNeg
                    var underLoadedAndOverLoadedFound = caseUnderLoadedAndOverLoaded();
                   System.out.println("underLoadedAndOverLoadedFound: " + underLoadedAndOverLoadedFound);


                    //UL
                    //check if a Under Loaded Dstores exsist
                    // Updates
                    //   toAdd
                    //   dStoresAdjustmentFactorPos
                    //   toRemove
                    //   dStoresFreeCapacityToLowerBound
                    var underLoadedAndFreeCapacityToLowerBound = false;
                    if (!underLoadedAndOverLoadedFound) {
                        underLoadedAndFreeCapacityToLowerBound = caseUnderLoaded();
                       System.out.println("underLoadedAndFreeCapacityToLowerBound: " + underLoadedAndFreeCapacityToLowerBound);

                    }


                    //OL
                    //check if a Over Loaded Dstores exsist
                    // Updates
                    //   toAdd
                    //   dStoresFreeCapacityToUpperBound
                    //   toRemove
                    //   dStoresAdjustmentFactorNeg
                    var overLoadedAndFreeCapacityToUpperBound = false;
                    if (!underLoadedAndOverLoadedFound) {
                        overLoadedAndFreeCapacityToUpperBound = caseOverLoaded();
                       System.out.println("overLoadedAndFreeCapacityToUpperBound: " + overLoadedAndFreeCapacityToUpperBound);

                    }
                }

                 /*System.out.println("\n\n\n[CONTROLLER]: End Of While");
           System.out.println("[CONTROLLER]: DStores Adjustment Neg: " + dStoresAdjustmentFactorNeg);
           System.out.println("[CONTROLLER]: DStores Adjustment Pos: " + dStoresAdjustmentFactorPos);
           System.out.println("[CONTROLLER]: DStores Free Capacity to Upper: " + dStoresFreeCapacityToUpperBound);
           System.out.println("[CONTROLLER]: DStores Free Capacity to Lower: " + dStoresFreeCapacityToLowerBound);
           System.out.println("\n[CONTROLLER]: Files Over Stored: " + fileOverStoredBy);
           System.out.println("[CONTROLLER]: Files Under Stored: " + fileUnderStoredBy);
           System.out.println("\n[CONTROLLER]: To Add: " + toAdd);
           System.out.println("[CONTROLLER]: To Remove: " + toRemove);*/


                // Form Messages

                if (toRemove.size() != 0 && toAdd.size() != 0) {

                   System.out.println("[CONTROLLER]: Forming Messages");


                    rebalanceLatch = new CountDownLatch(dstoreFileIndex.size());

                    while (rebalanceLatch == null) {
                    }

                    // For each Dstore in the system
                    for (Integer dStore : dstoreFileIndex.keySet()) {
                        var messageBuilder = new StringBuilder("REBALANCE ");
                        //p1 p2
                        var fileToSendToBuilder = new StringBuilder("");
                        //f1 2 p1 p2 (f2 3 p1 p2 p3)
                        var fileToSendBuilder = new StringBuilder("");

                        var filesToSendCounter = 0;
                        //var fileThatIsNeededElseWhereFound = false;

                        //for each file this dStore has
                        for (String originFileName : dstoreFileIndex.get(dStore).keySet()) {
                            if (dstoreFileIndex.get(dStore).get(originFileName) == FileState.STORED) {
                                // is this file need on another dStore?
                                if (toAdd.containsKey(originFileName)) {

                                    //fileThatIsNeededElseWhereFound = true;
                                    filesToSendCounter++;

                                    // build string of destination dstores for file transfer
                                    for (Integer dStoreDestination : toAdd.get(originFileName)) {
                                        fileToSendToBuilder.append(dStoreDestination + " ");
                                    }

                                    fileToSendBuilder.append(originFileName + " " + toAdd.get(originFileName).size() + " " + fileToSendToBuilder);

                                    // remove this file from toAdd as it has been delt with here
                                    toAdd.remove(originFileName);
                                }
                            }
                        }


                        messageBuilder.append(filesToSendCounter + " " + fileToSendBuilder);

                        // Append Remove message for this dStore

                        if (toRemove.containsKey(dStore)) {
                            //p1 p2
                            var filesToRemoveBuilder = new StringBuilder("");

                            for (String fileName : toRemove.get(dStore)) {
                                filesToRemoveBuilder.append(fileName + " ");
                            }

                            messageBuilder.append(toRemove.get(dStore).size() + " " + filesToRemoveBuilder);
                        }

                        if (!messageBuilder.toString().equals("REBALANCE 0 ")) {
                           System.out.println("[CONTROLLER]: sending REBALANCE message for this DStore");
                            // send the ReBalance message to the DStore
                            triggerDstoreListeners(messageBuilder.toString(), dStore);
                        } else {
                           System.out.println("[CONTROLLER]: No REBALANCE message for this DStore");
                            rebalanceLatch.countDown();
                        }

                    }


                    try {
                        if (!rebalanceLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                           System.out.println("[CONTROLLER]: REBALANCE FAILED");
                        }
                    } catch (InterruptedException e) {
                       System.out.println("[CONTROLLER]: REBALANCE FAILED");
                    }


                } else {
                   System.out.println("[CONTROLLER]: DStores Already Balanced");
                }

               System.out.println("[CONTROLLER]: ReBalance Complete: ");

               System.out.println("------" + dstoreFileIndex);
               System.out.println("------" + fileStoredLocations);
               System.out.println("------" + controllerIndex);
               System.out.println("------" + fileSizes);
               System.out.println("------" + connectedDstores);
               System.out.println("------" + dstoreConnectionToPortLookUp);
               System.out.println("------" + rebalanceAudits);

            }
            else{
               System.out.println("[REBALANCE] Skipped (not enough Files)");
            }
        }
        else {
           System.out.println("[REBALANCE] Skipped (not enough Dstores)");
        }

        rebalanceActive = false;
    }
    
    private void filterRemovedFiles(){
        
        for ( Integer dStore: rebalanceAudits.keySet() ) {
            for ( String fileName : rebalanceAudits.get(dStore)) {
                if (!controllerIndex.containsKey(fileName) ){
                    
                    rebalanceAudits.get(dStore).remove(fileName);
                    addToToRemove(dStore, fileName);
                    
                }else if (controllerIndex.get(fileName) != FileState.STORED){
                    
                    rebalanceAudits.get(dStore).remove(fileName);
                    addToToRemove(dStore, fileName);
                }
            }
        }
    }

    private synchronized void updateControllerWithDStoreAudits(ConcurrentHashMap<Integer, ArrayList<String>> rebalanceAudits){
        ConcurrentHashMap<Integer, Map<String, FileState>> newdstoreFileIndex = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, ArrayList<Integer>> newFileStoredLocations = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, FileState> newControllerIndex = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Integer> newFileSizes = new ConcurrentHashMap<>();
        List<Socket> newConnectedDstores = Collections.synchronizedList(new ArrayList<>());
        ConcurrentHashMap<Integer,Integer> newdstoreConnectionToPortLookUp = new ConcurrentHashMap<>();
        //ConcurrentHashMap<Integer, ArrayList<String>> rebalanceAudits = new ConcurrentHashMap<>();


        // Updating dstoreFileIndex
        // Coppy all FileState.Storing from old to new
        /*for ( Integer dStore : dstoreFileIndex.keySet()) {
            for ( String fileName: dstoreFileIndex.get(dStore).keySet()) {

                if (dstoreFileIndex.get(dStore).get(fileName) == FileState.STORING){

                    if (!newdstoreFileIndex.containsKey(dStore)) {
                        newdstoreFileIndex.put(dStore, new HashMap<>());
                    }
                    newdstoreFileIndex.get(dStore).put(fileName, FileState.STORING);
                }

            }

        }*/
        for ( Integer dStore : rebalanceAudits.keySet()) {
            newdstoreFileIndex.put(dStore, new HashMap<>());
            for ( String fileName: rebalanceAudits.get(dStore)) {
                newdstoreFileIndex.get(dStore).put(fileName, FileState.STORED);
            }
        }
        dstoreFileIndex = newdstoreFileIndex;


        // Updating FileStoredLocations
        for ( Integer dStore : rebalanceAudits.keySet()) {
            for ( String fileName: rebalanceAudits.get(dStore)) {
                if (!newFileStoredLocations.containsKey(fileName)) {
                    newFileStoredLocations.put(fileName, new ArrayList<>());
                }
                newFileStoredLocations.get(fileName).add(dStore);
            }
        }
        fileStoredLocations = newFileStoredLocations;

        // Updating ControllerIndex
        for (Integer dStore : rebalanceAudits.keySet()) {
            for (String fileName : rebalanceAudits.get(dStore)) {

                newControllerIndex.put(fileName, FileState.STORED);
            }
        }
        controllerIndex = newControllerIndex;

        // Updating FileSizes
        for ( String fileName1: fileSizes.keySet() ) {
            for (Integer dStore: rebalanceAudits.keySet() ) {
                for ( String fileName2: rebalanceAudits.get(dStore)) {
                    if (fileName1.equals(fileName2) && !newFileSizes.containsKey(fileName1)){
                        newFileSizes.put(fileName1, fileSizes.get(fileName1));
                    }
                }
            }
        }
        fileSizes = newFileSizes;


        // Update connectedDstores
        for ( Integer dStore : rebalanceAudits.keySet()) {
            for ( Socket dStoreSocket: connectedDstores ) {
                if (dstoreConnectionToPortLookUp.get(dStoreSocket.getPort()).equals(dStore)){
                    newConnectedDstores.add(dStoreSocket);
                }
            }
        }
        connectedDstores = newConnectedDstores;


        //update dstoreConnectionToPortLookUp ?????????????
        for ( Socket dStoreSocket: newConnectedDstores) {
            if (dstoreConnectionToPortLookUp.containsKey( dStoreSocket.getPort() )){
                newdstoreConnectionToPortLookUp.put( dStoreSocket.getPort(), dstoreConnectionToPortLookUp.get(dStoreSocket.getPort()) );
            }
        }
        dstoreConnectionToPortLookUp = newdstoreConnectionToPortLookUp;


    }

    private void checkFilesReplicatedRTimes(){
        //Is each file Replicated R times over the Dstores??

        fileOverStoredBy.clear();
        fileUnderStoredBy.clear();

        int fileStoredCount;

        //System.out.println("\n\n[CONTROLLER]: Checking file Stored Counts");

        for ( String fileName: controllerIndex.keySet()) {

            //System.out.println("Checking file: " + fileName);
            if (controllerIndex.get( fileName ) == FileState.STORED){

                //System.out.println("STORED");
                fileStoredCount = 0;

                for ( ArrayList<String> dStoreFilesStored: rebalanceAudits.values()) {
                    // for each dStore
                    //System.out.println("Checking in: " + dStoreFilesStored);
                    if (dStoreFilesStored.contains(fileName)){
                        fileStoredCount++;
                        //System.out.println("Found, Count is now: " + fileStoredCount);
                    }
                }

                if (fileStoredCount < rNum){
                    fileUnderStoredBy.put(fileName, (rNum - fileStoredCount));

                } else if (fileStoredCount > rNum){
                    fileOverStoredBy.put(fileName, (fileStoredCount - rNum));
                }
            }

        }

        /*System.out.println("\n[CONTROLLER]: Files Over Stored: " + fileOverStoredBy);
       System.out.println("[CONTROLLER]: Files Under Stored: " + fileUnderStoredBy);*/
    }

    private void CheckFilesEvenlyDistributedAmongDStores(){
        //we need to add Files from dStore
        dStoresAdjustmentFactorPos.clear();
        // we need to remove files from dStore
        dStoresAdjustmentFactorNeg.clear();

        // how many files can we add or remove untill we are on teh boundary (cant add or remove 1 more)
        dStoresFreeCapacityToUpperBound.clear();
        dStoresFreeCapacityToLowerBound.clear();
        int dStoreFileCount;

        //System.out.println("\n\n\n[CONTROLLER]: Checking File Distribution");
        int lowerBoundStoreLimit = (int) Math.floor( (float) (rNum * fileStoredLocations.size()) / connectedDstores.size());
        int upperBoundStoreLimit = (int) Math.ceil( (float) (rNum * fileStoredLocations.size()) / connectedDstores.size());
        int boundStoreLimitRange = upperBoundStoreLimit - lowerBoundStoreLimit;

       /*System.out.println("[CONTROLLER]: Lower Bound is: " + lowerBoundStoreLimit);
       System.out.println("[CONTROLLER]: Upper Bound is: " + upperBoundStoreLimit);*/

        for ( Integer dStore : dstoreFileIndex.keySet() ) {

            dStoreFileCount = 0;

            for (FileState fileState : dstoreFileIndex.get(dStore).values()) {
                if (fileState == FileState.STORED) {
                    dStoreFileCount++;
                }
            }

            if (dStoreFileCount > upperBoundStoreLimit) {
                //dStoresAdjustmentFactor.put(dStore, (upperBoundStoreLimit - dStoreFileCount));
                dStoresAdjustmentFactorNeg.put(dStore, (dStoreFileCount - upperBoundStoreLimit));
                dStoresFreeCapacityToLowerBound.put(dStore, dStoreFileCount - lowerBoundStoreLimit);

            } else if (dStoreFileCount < lowerBoundStoreLimit) {
                //dStoresAdjustmentFactor.put(dStore, (lowerBoundStoreLimit - dStoreFileCount));
                dStoresAdjustmentFactorPos.put(dStore, (lowerBoundStoreLimit - dStoreFileCount));
                dStoresFreeCapacityToUpperBound.put(dStore, upperBoundStoreLimit - dStoreFileCount);

            } else if (dStoreFileCount == upperBoundStoreLimit && boundStoreLimitRange != 0 ) {
                dStoresFreeCapacityToLowerBound.put(dStore, dStoreFileCount - lowerBoundStoreLimit);

            } else if (dStoreFileCount == lowerBoundStoreLimit && boundStoreLimitRange != 0){
                dStoresFreeCapacityToUpperBound.put(dStore, upperBoundStoreLimit - dStoreFileCount);

            }else if (boundStoreLimitRange != 0){
                dStoresFreeCapacityToUpperBound.put(dStore, upperBoundStoreLimit - dStoreFileCount);
                dStoresFreeCapacityToLowerBound.put(dStore, dStoreFileCount - lowerBoundStoreLimit);
            }

            //if 1 dstore is under loaded there isnt nesseccary an over loaded Store and Vice versa
        }

      /*System.out.println("\n[CONTROLLER]: Finished caculating Distribution adjustments");
       System.out.println("[CONTROLLER]: DStores Adjustment Neg: " + dStoresAdjustmentFactorNeg);
       System.out.println("[CONTROLLER]: DStores Adjustment Pos: " + dStoresAdjustmentFactorPos);
       System.out.println("[CONTROLLER]: DStores Free Capacity to Upper: " + dStoresFreeCapacityToUpperBound);
       System.out.println("[CONTROLLER]: DStores Free Capacity to Lower: " + dStoresFreeCapacityToLowerBound);*/

    }

    private boolean caseOverStoredAndOverLoaded(){
        // OS &  OL
        // check if dstore is overLoaded and has a file which is over stored
        var overStoredAndOverLoadedFound = false;

        try {
            for (Integer dStore : dStoresAdjustmentFactorNeg.keySet()) {
                for (String fileName : fileOverStoredBy.keySet()) {
                    if (dstoreFileIndex.get(dStore).containsKey(fileName)) {

                        overStoredAndOverLoadedFound = true;

                        addToToRemove(dStore, fileName);

                        decrementMapKey(dStoresAdjustmentFactorNeg, dStore);
                        decrementMapKey(fileOverStoredBy, fileName);
                        decrementMapKey(dStoresFreeCapacityToLowerBound, dStore);


                        break;
                    }
                }
            }
        } catch (ConcurrentModificationException ignored){}

        return overStoredAndOverLoadedFound;
    }

    private boolean caseOverStored(){
        //OS
        //check if dstore has an over stored file and removing it wont go over the lowerBound threshold
        var overStoredAndAboveLowerBoundThreshHoldLoadedFound = false;

        try {
            for ( Integer dStore : dStoresFreeCapacityToLowerBound.keySet()) {
                for (String fileName : fileOverStoredBy.keySet()) {

                    if (dstoreFileIndex.get(dStore).containsKey(fileName)) {

                        overStoredAndAboveLowerBoundThreshHoldLoadedFound = true;

                        addToToRemove(dStore, fileName);

                        decrementMapKey(dStoresFreeCapacityToLowerBound, dStore);
                        incrementMapKey(dStoresFreeCapacityToUpperBound, dStore);
                        decrementMapKey(fileOverStoredBy, fileName);

                        break;
                    }
                }
            }

        } catch (ConcurrentModificationException ignored){}

        return overStoredAndAboveLowerBoundThreshHoldLoadedFound;
    }

    private boolean caseUnderStoredAndUnderLoaded(){
        // US  &  UL
        // check if an under stored file can be added to a under loaded dstore
        var underStoredAndUnderLoadedFound = false;

        try {
            for (Integer dStore : dStoresAdjustmentFactorPos.keySet()) {
                for (String fileName : fileUnderStoredBy.keySet()) {
                    if (!dstoreFileIndex.get(dStore).containsKey(fileName)) {

                        underStoredAndUnderLoadedFound = true;

                        addToToAdd(dStore, fileName);

                        decrementMapKey(dStoresAdjustmentFactorPos, dStore);
                        decrementMapKey(fileUnderStoredBy, fileName);
                        decrementMapKey(dStoresFreeCapacityToUpperBound, dStore);


                        break;
                    }
                }
            }
        } catch (ConcurrentModificationException ignored){}

        return underStoredAndUnderLoadedFound;
    }

    private boolean caseUnderStored(){
        // US
        // check if an under stored file can be added to a dstore with out going over the upper bound threshold
        var underStoredAndBelowUpperBoundThreshHoldLoadedFound = false;
        try {
            for ( Integer dStore : dStoresFreeCapacityToUpperBound.keySet()) {
                for (String fileName : fileUnderStoredBy.keySet()) {

                    if (!dstoreFileIndex.get(dStore).containsKey(fileName)) {

                        underStoredAndBelowUpperBoundThreshHoldLoadedFound = true;

                        addToToAdd(dStore, fileName);

                        decrementMapKey(dStoresFreeCapacityToUpperBound,dStore);
                        incrementMapKey(dStoresFreeCapacityToLowerBound, dStore);
                        decrementMapKey(fileUnderStoredBy, fileName);

                        break;
                    }
                }
            }

        } catch (ConcurrentModificationException ignored){}


        return underStoredAndBelowUpperBoundThreshHoldLoadedFound;
    }

    private boolean caseUnderLoadedAndOverLoaded(){
        // UL  & OL
        // check if a pair of Over loaded and a Under Loaded Dstores exsist
        var underLoadedAndOverLoadedFound = false;

        try {
            // find a pair of oLdStore and a uLdStore st
            for (Integer oLdStore : dStoresAdjustmentFactorNeg.keySet()) {
                for (Integer uLdStore : dStoresAdjustmentFactorPos.keySet()) {

                    // the oLdStore contains a Stored file that the uLdStore doesnt
                    for (String fileName : dstoreFileIndex.get(oLdStore).keySet()) {
                        if (dstoreFileIndex.get(oLdStore).get(fileName) == FileState.STORED && !dstoreFileIndex.get(uLdStore).containsKey(fileName)) {

                            underLoadedAndOverLoadedFound = true;

                            addToToAdd(uLdStore, fileName);
                            decrementMapKey(dStoresAdjustmentFactorPos, uLdStore);
                            decrementMapKey(dStoresFreeCapacityToUpperBound, uLdStore);

                            addToToRemove(oLdStore, fileName);
                            decrementMapKey(dStoresAdjustmentFactorNeg, oLdStore);
                            decrementMapKey(dStoresFreeCapacityToLowerBound, oLdStore);



                            break;
                        }
                    }

                }
            }
        } catch (ConcurrentModificationException ignored){}

        return underLoadedAndOverLoadedFound;
    }

    private boolean caseUnderLoaded(){
        // UL
        // check if a Under Loaded Dstores exsist
        var underLoadedAndFreeCapacityToLowerBound = false;
        try {
            // find a pair of a uLdStore and a dStore with free capacity to lower bound st
            for (Integer uLdStore : dStoresAdjustmentFactorPos.keySet()) {
                for (Integer freeCapacityToLowerBoundDStore : dStoresFreeCapacityToLowerBound.keySet()) {

                    // the uLdStore contains a Stored file that the freeCapacityToLowerBoundDStore doesnt
                    for (String fileName : dstoreFileIndex.get(uLdStore).keySet()) {
                        if (dstoreFileIndex.get(uLdStore).get(fileName) == FileState.STORED && !dstoreFileIndex.get(freeCapacityToLowerBoundDStore).containsKey(fileName)) {

                            underLoadedAndFreeCapacityToLowerBound = true;

                            addToToAdd(uLdStore, fileName);
                            decrementMapKey(dStoresAdjustmentFactorPos, uLdStore);
                            decrementMapKey(dStoresFreeCapacityToUpperBound, uLdStore);


                            addToToRemove(freeCapacityToLowerBoundDStore, fileName);
                            decrementMapKey(dStoresFreeCapacityToLowerBound, freeCapacityToLowerBoundDStore);
                            incrementMapKey(dStoresFreeCapacityToUpperBound, freeCapacityToLowerBoundDStore);


                            break;
                        }
                    }
                }
            }
        } catch (ConcurrentModificationException ignored) { }


        return underLoadedAndFreeCapacityToLowerBound;
    }

    private boolean caseOverLoaded(){
        // OL
        // check if a Over Loaded Dstores exsist
        var overLoadedAndFreeCapacityToUpperBound = false;
        try {
            // find a pair of a uLdStore and a dStore with free capacity to lower bound st
            for (Integer oLdStore : dStoresAdjustmentFactorNeg.keySet()) {
                for (Integer freeCapacityToUpperBoundDStore : dStoresFreeCapacityToUpperBound.keySet()) {

                    // the uLdStore contains a Stored file that the freeCapacityToLowerBoundDStore doesnt
                    for (String fileName : dstoreFileIndex.get(oLdStore).keySet()) {
                        if (dstoreFileIndex.get(oLdStore).get(fileName) == FileState.STORED && !dstoreFileIndex.get(freeCapacityToUpperBoundDStore).containsKey(fileName)) {

                            overLoadedAndFreeCapacityToUpperBound = true;

                            addToToAdd(freeCapacityToUpperBoundDStore, fileName);
                            decrementMapKey(dStoresFreeCapacityToUpperBound, freeCapacityToUpperBoundDStore);
                            incrementMapKey(dStoresFreeCapacityToLowerBound, freeCapacityToUpperBoundDStore);

                            addToToRemove(oLdStore, fileName);
                            decrementMapKey(dStoresAdjustmentFactorNeg, oLdStore);
                            decrementMapKey(dStoresFreeCapacityToLowerBound, oLdStore);


                            break;
                        }
                    }
                }
            }
        } catch (ConcurrentModificationException ignored) { }


        return overLoadedAndFreeCapacityToUpperBound;
    }




}
