import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

public class DstoreClientHandler extends Thread implements CommunicationReceivedListener {

    private Socket clientSocket;
    private Dstore dStore;
    private PrintWriter pw = null;
    private BufferedReader br = null;
    private OutputStream out = null;
    private InputStream in = null;
    private Logger logger = Logger.getLogger(String.valueOf(DstoreClientHandler.class));
    private String fileFolder;

    public DstoreClientHandler(Socket clientSocket, Dstore dStore, String fileFolder) {
        setClientSocket( clientSocket );
        this.dStore = dStore;
        this.fileFolder = fileFolder;
        dStore.addListener(this);
    }


    public void setClientSocket(Socket clientSocket){
        this.clientSocket = clientSocket;
    }

    @Override
    public void run () {


        System.out.println( "[" + this.getName() + " Client]" +  "Device Connected to Dstore Handler on Port: " + clientSocket.getPort());

        try {
            pw = new PrintWriter(clientSocket.getOutputStream(), true);
            br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            in = clientSocket.getInputStream();
            out = clientSocket.getOutputStream();

        } catch (IOException e) {
            e.printStackTrace();
        }


        String[] lineSplit = null;
        while (true) {
            String line = null;

            /*System.out.println("-------------");
            System.out.println(clientSocket.isConnected());
            System.out.println(clientSocket.isClosed());*/

            try {
                if ((line = br.readLine()) != null) {

                    System.out.println( "[" + this.getName() + " Client]" +  "MESSAGE: " + line);

                    // Message Parsing
                    lineSplit = line.split(" ");
                    switch (lineSplit[0]) {


                        /*case "disconnect" -> {
                            System.out.println( "[" + this.getName() + " Client ]" +  "Message Received: " + line + " On Port: " + clientSocket.getPort());
                            disconnect();
                            break;
                        }*/


                        case "STORE" -> {
                            String fileName = lineSplit[1];
                            int fileSize = Integer.parseInt(lineSplit[2]);

                            System.out.println( "[" + this.getName() + " Client]" +  "STORE COMMAND RECEIVED, FILE: " + fileName + " Size: " + fileSize);

                            // check it can store the file
                            pw.println("ACK");

                            System.out.println( "[" + this.getName() + " Client]" +  "RESPONDED ACK");


                            File fileToWrite = new File( fileFolder + File.separator + fileName );
                            fileToWrite.createNewFile();
                            fileToWrite.setWritable(true);

                            /*if ( !fileToWrite.exists() ){
                                fileToWrite.mkdirs();
                            }*/

                            OutputStream fileOut = new FileOutputStream( fileToWrite );

                            byte[] bytes;

                            bytes = in.readNBytes( fileSize );

                            fileOut.write(bytes);
                            fileOut.flush();

                            fileOut.close();

                            System.out.println( "[" + this.getName() + " Client]" +  "FILE 'SUCCESSFULLY' STORED");

                            dStore.triggerControllerSendMessage("STORE_ACK "  + fileName);

                            dStore.files.put(fileName, fileSize);
                        }

                        case "REBALANCE_STORE" -> {
                            String fileName = lineSplit[1];
                            int fileSize = Integer.parseInt(lineSplit[2]);

                            System.out.println( "[" + this.getName() + " Client]" +  "REBALANCE_STORE COMMAND RECEIVED, FILE: " + fileName + " Size: " + fileSize);

                            // check it can store the file
                            pw.println("ACK");

                            System.out.println( "[" + this.getName() + " Client]" +  "RESPONDED ACK");

                            //make directories of they dont allready exist
                            Files.createDirectories( Paths.get( fileFolder ) );

                            File fileToWrite = new File( fileFolder + File.separator + fileName );
                            fileToWrite.createNewFile();
                            fileToWrite.setWritable(true);

                            /*if ( !fileToWrite.exists() ){
                                fileToWrite.mkdirs();
                            }*/

                            OutputStream fileOut = new FileOutputStream( fileToWrite );

                            byte[] bytes;

                            bytes = in.readNBytes( fileSize );

                            fileOut.write(bytes);
                            fileOut.flush();

                            fileOut.close();

                            System.out.println( "[" + this.getName() + " Client]" +  "FILE 'SUCCESSFULLY' STORED");

                            dStore.files.put(fileName, fileSize);

                        }


                        case "LOAD_DATA" -> {
                            String fileName = lineSplit[1];

                            System.out.println("[" + this.getName() + " Client]" + "LOAD COMMAND RECEIVED: " + fileName + " Loading from: " + Path.of(fileFolder + File.pathSeparator + fileName) + " : " + fileFolder + File.pathSeparator + fileName);
                            System.out.println("[" + this.getName() + " Client]" + "May Loading from: " + Path.of(fileFolder + File.separator + fileName) + " : " + fileFolder + File.separator + fileName);

                            // if file to load doesnt exist close socket;
                            //File fileToWrite = new File( fileFolder + File.separator + fileName );

                            File f = new File(fileFolder + File.separator + fileName);
                            if(!f.exists() || f.isDirectory()) {
                                System.out.println("[" + this.getName() + " Client]" + "LOAD NON-EXSISAINT FILE -> DISCONNECT");

                                disconnect();
                                break;
                            }


                            out.write(Files.readAllBytes(Paths.get(fileFolder + File.separator + fileName)));


                            System.out.println("[" + this.getName() + " Client]" + "LOAD COMMAND Finished");
                        }


                        /*case "REMOVE" -> {
                            String fileName = lineSplit[1];
                            File fileToDelete = new File(fileFolder + File.pathSeparator + fileName);
                            if (!fileToDelete.exists()) {
                                pw.println("ERROR_FILE_DOES_NOT_EXIST " + fileName); // To Controller
                            } else {
                                fileToDelete.delete();
                                pw.println("REMOVE_ACK " + fileName); // To Controller
                            }
                        }*/


                        default -> {
                            // LOG ME
                            System.out.println( "[" + this.getName() + " Client]" +  "Message Received: " + line + " On Port: " + clientSocket.getPort());
                            System.out.println( "[" + this.getName() + " Client]" +  "Sending Message: " + line + "'" + " On Port: " + clientSocket.getPort());
                            pw.println(line + "'");

                        }
                    }

                    //System.out.println( "[" + this.getName() + " Client]" +  "OUT OF SWITCH");

                }else {
                    //peer Closed Socket
                    System.out.println("[" + this.getName() + " Client] " +"Client Closed Socket");
                    break;
                }
            } catch (IOException e){
                System.out.println( "[" + this.getName() + " Client] " +  "CLIENT TIMEOUT ON PORT: " + clientSocket.getPort() + " DISCONNECTING");
                disconnect();
                break;
            }


        }

        /*System.out.println(clientSocket.isConnected());
        //System.out.println(clientSocket.isClosed());
        System.out.println( "[" + this.getName() + " Client ]" +  "Socket Closed" + " On Port: " + clientSocket.getPort());*/

        System.out.println( "[" + this.getName() + " Client] Out of While True -> DISCONNECTING");

        disconnect();

    }

    private void disconnect() {
        System.out.println( "[" + this.getName() + " Client]" +  "DISCONNECTING");

        try {
            if (clientSocket != null) {
                //pw.println("disconnect");
                clientSocket.close();
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


        try {
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
        }
    }

    @Override
    public void receivedMessage(String message, int dStorePort) {
        System.out.println( "[" + this.getName() + " Client]" +  "DSTORE RECEIVED MESSAGE FORM CONTROLLER: " + message);
    }
}


