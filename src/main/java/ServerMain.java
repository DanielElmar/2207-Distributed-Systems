import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class ServerMain {

    //javac -cp cw2207;. cw2207\Controller.java
    //java -cp cw2207;. cw2207/Controller 1256 5 8000 8000

    private static Logger logger = Logger.getLogger(String.valueOf(ServerMain.class));

    protected ConcurrentHashMap<Integer, Map<String, FileState>> dstoreFileIndex = new ConcurrentHashMap<>();

    protected ConcurrentHashMap<String, FileState> controllerIndex = new ConcurrentHashMap<>();

    protected List<Integer> connectedDstores = Collections.synchronizedList(new ArrayList<>());

    protected ConcurrentHashMap<String, ArrayList<Integer>> fileStoredLocations = new ConcurrentHashMap<>();

    protected int rNum = 2;

    protected ConcurrentHashMap<Integer, ArrayList<String>> rebalanceAudits = new ConcurrentHashMap<>();

    private final HashMap<String, Integer> fileOverStoredBy = new HashMap<String, Integer>();
    private final HashMap<String, Integer> fileUnderStoredBy = new HashMap<String, Integer>();

    //we need to add Files from dStore
    private final HashMap<Integer, Integer> dStoresAdjustmentFactorPos = new HashMap<>();
    // we need to remove files from dStore
    private final HashMap<Integer, Integer> dStoresAdjustmentFactorNeg = new HashMap<>();

    // how many files can we add or remove untill we are on teh boundary (cant add or remove 1 more)
    private final HashMap<Integer, Integer> dStoresFreeCapacityToUpperBound = new HashMap<>();
    private final HashMap<Integer, Integer> dStoresFreeCapacityToLowerBound = new HashMap<>();


    HashMap<Integer, ArrayList<String>> toRemove = new HashMap<>();
    HashMap<Integer, ArrayList<String>> toAdd = new HashMap<>();

    private void addToToRemove(Integer key, String fileName){
        if (toRemove.containsKey(key)){
            toRemove.get(key).add(fileName);
        }else{
            toRemove.put(key, new ArrayList<>(Collections.singletonList(fileName)));
        }
    }

    private void addToToAdd(Integer key, String fileName){
        if (toAdd.containsKey(key)){
            toAdd.get(key).add(fileName);
        }else{
            toAdd.put(key, new ArrayList<>(Collections.singletonList(fileName)));
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

    public static void main(String[] args) {
        int port;
        int rNum;
        int timeout;
        int rebalancePeriod;

        if ( args.length == 4) {
             port = Integer.parseInt(args[0]);
             rNum = Integer.parseInt(args[1]);
             timeout = Integer.parseInt(args[2]);
             rebalancePeriod = Integer.parseInt(args[3]);
        }else{
             port = 1256;
             rNum = 2;
             timeout = 5000;
             rebalancePeriod = 80000;
        }



        /*
        dstoreFileIndex
        controllerIndex
        connectedDstores
        fileStoredLocations
        rebalanceAudits
         */


        try {
            System.out.println(Files.readAllBytes(Paths.get("dstore1" + File.separator + "file_to_store")).length);
        } catch (IOException e) {
            e.printStackTrace();
        }


        /*var main = new ServerMain();

        main.rNum = 3;

        var dStore1Map = new HashMap<String, FileState>();
        dStore1Map.put("file1", FileState.STORED);
        dStore1Map.put("file2", FileState.STORED);
        //dStore1Map.put("file4", FileState.STORED);


        var dStore2Map = new HashMap<String, FileState>();
        dStore2Map.put("file2", FileState.STORED);
        dStore2Map.put("file3", FileState.STORED);
        //dStore2Map.put("file4", FileState.STORED);

        var dStore3Map = new HashMap<String, FileState>();
        dStore3Map.put("file1", FileState.STORED);
        dStore3Map.put("file3", FileState.STORED);


        main.dstoreFileIndex.put(1, dStore1Map);
        main.dstoreFileIndex.put(2, dStore2Map);
        main.dstoreFileIndex.put(3, dStore3Map);


        main.controllerIndex.put("file1", FileState.STORED);
        main.controllerIndex.put("file2", FileState.STORED);
        main.controllerIndex.put("file3", FileState.STORED);
       // main.controllerIndex.put("file4", FileState.STORED);

        Collections.addAll(main.connectedDstores, 1,2,3);

        main.fileStoredLocations.put("file1", new ArrayList<>(Arrays.asList(1,3)) );
        main.fileStoredLocations.put("file2", new ArrayList<>(Arrays.asList(1,2)) );
        main.fileStoredLocations.put("file3", new ArrayList<>(Arrays.asList(2,3)) );
        //main.fileStoredLocations.put("file4", new ArrayList<>(Arrays.asList(1,2)) );

        main.rebalanceAudits.put(1, new ArrayList<>(Arrays.asList("file1", "file2")) );
        main.rebalanceAudits.put(2, new ArrayList<>(Arrays.asList("file2", "file3")) );
        main.rebalanceAudits.put(3, new ArrayList<>(Arrays.asList("file1", "file3")) );

        main.triggerRebalance();*/



        /*for (Integer ints : myHashMap.keySet()) {
            if (ints == 5){
                myHashMap.remove(5);
            }else{
                System.out.println( ints );
            }

        }

        System.out.println(myHashMap);*/
        /*HashMap<Integer, Map<String, FileState>> dstoreFileIndex = new HashMap<>();

        dstoreFileIndex.put(1, new HashMap<String, FileState>());
        dstoreFileIndex.get(1).put("F1", FileState.STORED);
        dstoreFileIndex.get(1).put("F2", FileState.STORED);
        dstoreFileIndex.get(1).put("F3", FileState.STORED);
        dstoreFileIndex.get(1).put("F4", FileState.STORING);
        dstoreFileIndex.get(1).put("F5", FileState.REMOVING);

        for ( FileState fileState : dstoreFileIndex.get(1).values() ) {
            System.out.println(fileState);
        }


        int lowerBound = (int) Math.floor( (float) 3*10 / 4);
        int upperBound = (int) Math.ceil( (float) 3*10 / 4);

        System.out.println( (float) 3*10 / 4);
        System.out.println(lowerBound);
        System.out.println(upperBound);*/
        /*try {
            var controllerSocket = new Socket( "127.0.0.1", 12345);
            var pw = new PrintWriter(controllerSocket.getOutputStream(), true);
            var br = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));

            pw.println("REMOVE file_to_store");
            controllerSocket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }*/
        /*Map<String, ArrayList<Integer>> fileStoredLocations = new HashMap<>();
        String fileName = "file_to_store";
        Integer port3 = 12346;

        fileStoredLocations.put( fileName, new ArrayList<Integer>() );

        fileStoredLocations.get( fileName ).add( port3 );

        System.out.println( fileStoredLocations.toString() );//*/
        /*ArrayList<Integer> waitingForStoreAckFromPorts = new ArrayList<>();
        waitingForStoreAckFromPorts.add( 120 );

        System.out.println(waitingForStoreAckFromPorts.get(0));

        waitingForStoreAckFromPorts.remove((Integer) 200);

        System.out.println(waitingForStoreAckFromPorts.get(0));

        waitingForStoreAckFromPorts.remove((Integer) 120);

        System.out.println(waitingForStoreAckFromPorts.size());*/
        /*int[] intPortsToStoreTo = new int[]{123,124,125};
        StringBuilder stringPortsToStoreTo = new StringBuilder();

        for (int port2 : intPortsToStoreTo) {
            stringPortsToStoreTo.append(port2).append(" ");
        }

        System.out.println( intPortsToStoreTo[0] );
        System.out.println( stringPortsToStoreTo );*/
        /*String fileFolder = "dstore1";
        String fileName = "test_file";
        int fileSize = 28;

        //make directories of they dont allready exist
        try {
            System.out.println( File.pathSeparator );
            System.out.println( File.separator );
            System.out.println( File.pathSeparatorChar );
            System.out.println( File.separatorChar );

            Files.createDirectories( Paths.get( fileFolder ) );
            File fileToWrite = new File( fileFolder + File.separator + fileName );
            fileToWrite.createNewFile();
            fileToWrite.setWritable(true);

        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }


    protected synchronized void triggerRebalance() {

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

        toRemove.clear();
        toAdd.clear();

        System.out.println("\n\n\nBalancing Phase");


        while ( dStoresAdjustmentFactorPos.size() != 0 || dStoresAdjustmentFactorNeg.size() != 0 || fileUnderStoredBy.size() != 0 || fileOverStoredBy.size() != 0 ) {
            System.out.println("\n\nNew While Cycle");
            System.out.println("DStores Adjustment Neg: " + dStoresAdjustmentFactorNeg);
            System.out.println("DStores Adjustment Pos: " + dStoresAdjustmentFactorPos);
            System.out.println("DStores Free Capacity to Upper: " + dStoresFreeCapacityToUpperBound);
            System.out.println("DStores Free Capacity to Lower: " + dStoresFreeCapacityToLowerBound);
            System.out.println("Files Over Stored: " + fileOverStoredBy);
            System.out.println("Files Under Stored: " + fileUnderStoredBy);
            System.out.println("To Add: " + toAdd);
            System.out.println("To Remove: " + toRemove);


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
            if ( !overStoredAndOverLoadedFound ){
                overStoredAndAboveLowerBoundThreshHoldLoadedFound = caseOverStored();
                System.out.println("overStoredAndAboveLowerBoundThreshHoldLoadedFound: " + overStoredAndAboveLowerBoundThreshHoldLoadedFound);
            }

            /*
            // if file still over stored just remove from a random dstore, making it under loaded

            if ( !overStoredAndOverLoadedFound && !overStoredAndAboveLowerBoundThreshHoldLoadedFound ){
                logger.info("FILE STILL OVER STORED, ALL DSTORES AT LOWER STORE LIMIT. UNDER LOADING RANDOM DSTORE");
                for ( Integer dStore : dstoreFileIndex.keySet()) {
                    for (String fileName : fileOverStoredBy.keySet()) {

                        if (dstoreFileIndex.get(dStore).containsKey(fileName)) {
                            addToToRemove(dStore, fileName);

                            if (dStoresFreeCapacityToLowerBound.get(dStore) > 1) {
                                dStoresFreeCapacityToLowerBound.put(dStore, (dStoresFreeCapacityToLowerBound.get(dStore) - 1));
                            } else {
                                dStoresFreeCapacityToLowerBound.remove(dStore);
                            }

                            if (fileOverStoredBy.get(fileName) > 1) {
                                fileOverStoredBy.put(fileName, fileOverStoredBy.get(fileName) - 1);
                            } else {
                                fileOverStoredBy.remove(fileName);
                            }
                            break;
                        }
                    }
                }
            }*/


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
            if ( !underStoredAndUnderLoadedFound ){
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

        System.out.println("\n\n\nEnd Of While");
        System.out.println("DStores Adjustment Neg: " + dStoresAdjustmentFactorNeg);
        System.out.println("DStores Adjustment Pos: " + dStoresAdjustmentFactorPos);
        System.out.println("DStores Free Capacity to Upper: " + dStoresFreeCapacityToUpperBound);
        System.out.println("DStores Free Capacity to Lower: " + dStoresFreeCapacityToLowerBound);
        System.out.println("\nFiles Over Stored: " + fileOverStoredBy);
        System.out.println("Files Under Stored: " + fileUnderStoredBy);
        System.out.println("\nTo Add: " + toAdd);
        System.out.println("To Remove: " + toRemove);

        // Form Messages

        //to add

    }

    private void checkFilesReplicatedRTimes(){
        //Is each file Replicated R times over the Dstores??

        fileOverStoredBy.clear();
        fileUnderStoredBy.clear();

        int fileStoredCount;

        System.out.println("\n\nChecking file Stored Counts");

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

        System.out.println("\nFiles Over Stored: " + fileOverStoredBy);
        System.out.println("Files Under Stored: " + fileUnderStoredBy);
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

        System.out.println("\n\n\nChecking File Distribution");
        int lowerBoundStoreLimit = (int) Math.floor( (float) (rNum * fileStoredLocations.size()) / connectedDstores.size());
        int upperBoundStoreLimit = (int) Math.ceil( (float) (rNum * fileStoredLocations.size()) / connectedDstores.size());
        int boundStoreLimitRange = upperBoundStoreLimit - lowerBoundStoreLimit;

        System.out.println("Lower Bound is: " + lowerBoundStoreLimit);
        System.out.println("Upper Bound is: " + upperBoundStoreLimit);

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

        System.out.println("\nFinished caculating Distribution adjustments");
        System.out.println("DStores Adjustment Neg: " + dStoresAdjustmentFactorNeg);
        System.out.println("DStores Adjustment Pos: " + dStoresAdjustmentFactorPos);
        System.out.println("DStores Free Capacity to Upper: " + dStoresFreeCapacityToUpperBound);
        System.out.println("DStores Free Capacity to Lower: " + dStoresFreeCapacityToLowerBound);

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
