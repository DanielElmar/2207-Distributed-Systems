import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class Dstore {

    private Logger logger = Logger.getLogger(String.valueOf(Dstore.class));
    private Socket controllerSocket = null;
    /*private PrintWriter pw = null;
    private BufferedReader br = null;
    private OutputStream out = null;
    private InputStream in = null;*/

    private ServerSocket serverSocket;
    private Socket ControllerSocket;
    protected int port;
    private int controllerPort;
    private int timeout;
    private String fileFolder;

    private List<Thread> connectedClientsThreads = Collections.synchronizedList(new ArrayList<>());
    private DstoreControllerHandler controllerConnectionThread;

    private List<CommunicationReceivedListener> listeners = Collections.synchronizedList(new ArrayList<>());
    private CommunicationSendListener communicationSendListener;

    // Updated on Store and Remove
    protected ConcurrentHashMap<String, Integer> files = new ConcurrentHashMap<>();


    public Dstore( int port, int controllerPort, int timeout, String fileFolder){
        this.port = port;
        this.controllerPort = controllerPort;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
        startDstore();
    }

    public static void main(String[] args) {

        if ( args.length == 4 ) {
            int port = Integer.parseInt(args[0]);
            int controllerPort = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            String fileFolder = args[3];


            Dstore dstore = new Dstore(port, controllerPort, timeout, fileFolder);


        }/*else{
            var thread1 = new Dstore(12346, 12345, 10000, "dstore1");
            var thread2 = new Dstore(12347, 12345, 10000, "dstore2");

            thread1.setName("DSTORE1");
            thread2.setName("DSTORE2");

            thread1.start();
            thread2.start();

            //new Dstore(12354, 12345, 10000, "../../resources/dstores/dstore2").start();;
            //Dstore dstore3 = new Dstore(12348, 12345, 1000, "../../resources/dstores/dstore3");
            //Dstore dstore4 = new Dstore(12349, 12345, 1000, "../../resources/dstores/dstore4");
        }*/
    }

    public void startDstore(){

        // delete exsisting folder
        File folder = new File(fileFolder);
        deleteFolder(folder);


        //make directories of they dont allready exist
        try {
            Files.createDirectories( Paths.get( fileFolder ) );
        } catch (IOException e) {
            //e.printStackTrace();
        }

        connectToController();
        lookForClientConnections();
    }

    private static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if(files != null) {
            for(File file: files) {
                if(file.isDirectory()) {
                    deleteFolder(file);
                } else {
                    file.delete();
                }
            }
        }
        folder.delete();
    }

    private void connectToController(){

        // Disconect
        //disconnectFromController();

        // connectToController to cw2207.Controller
        try {
            // form connection with InPut and Output streams
            controllerSocket = new Socket( "127.0.0.1", controllerPort); //, InetAddress.getByName("127.0.0.1"), port
            //controllerSocket.setSoTimeout(timeout);
            controllerSocket.setKeepAlive(true);

            System.out.println(" DSTORE] CONNECTED TO CONTROLLER" + " On Port: " + controllerPort);


            controllerConnectionThread = new DstoreControllerHandler(controllerSocket, this, timeout, fileFolder);
            controllerConnectionThread.start();


        } catch (IOException e) {
            e.printStackTrace();
        }

        //System.out.println(controllerSocket.isConnected());
        //System.out.println(controllerSocket.isClosed());
        //System.out.println("END");
    }//*/

    /*private void disconnectFromController(){
        try {
            if (controllerSocket != null) {
                pw.println("disconnect");
                controllerSocket.close();
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
            if ( in != null) {
                in.close();
            }
        } catch (IOException ignored) { }

        try {
            if (out != null) {
                out.close();
            }
        } catch (IOException ignored) { }

    }*/

    private void lookForClientConnections(){

        try {
            serverSocket = new ServerSocket(port);
            //serverSocket.setSoTimeout(timeout);
        } catch (IOException e) {
            e.printStackTrace();
        }


        while (true){
            try {
                //System.out.println("Looking for Connection");
                Socket clientSocket = serverSocket.accept();
                //clientSocket.setSoTimeout(timeout);

                Thread thread = new DstoreClientHandler(clientSocket, this, fileFolder );
                connectedClientsThreads.add( thread );
                thread.start();
            } catch (IOException ignored) {}
        }


        /*DstoreClientHandler runnable = new DstoreClientHandler(this, timeout, fileFolder);
        ArrayList<Thread> threads = new ArrayList<>();
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                //System.out.println("Looking for Connection");
                Thread thread = new Thread( runnable );
                threads.add( thread );
                runnable.setClientSocket( socket );
                thread.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/

    }

    protected synchronized void addListener( CommunicationReceivedListener listener){
        listeners.add(listener);
    }

    protected synchronized void triggerListeners(String message, int dStorePort){
        for ( CommunicationReceivedListener listener : listeners ) {
            listener.receivedMessage( message, dStorePort );
        }
    }

    protected synchronized void setControllerSendListener( CommunicationSendListener listener ){
        this.communicationSendListener = listener;
    }

    protected synchronized void triggerControllerSendMessage( String message ){
        communicationSendListener.sendMessage( message );
    }

    /*private void sendMessage( String message ){
        pw.println(message);
    }*/

}
