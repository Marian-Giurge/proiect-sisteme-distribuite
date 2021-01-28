
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import static java.lang.System.exit;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.List;

public class Main {

    static int regPort = Configurations.REG_PORT;

    static Registry registry;

    /**
     * respawns replica servers and register replicas at master
     *
     * @param master
     * @throws IOException
     */
    static void respawnReplicaServers(Master master) throws IOException {
        System.out.println("[@main] respawning replica servers ");
        try ( // TODO make file names global
                BufferedReader br = new BufferedReader(new FileReader("repServers.txt"))) {
            int n = Integer.parseInt(br.readLine().trim());
            ReplicaLoc replicaLoc;
            String s;
            
            for (int i = 0; i < n; i++) {
                s = br.readLine().trim();
                replicaLoc = new ReplicaLoc(i, s.substring(0, s.indexOf(':')), true);
                ReplicaServer rs = new ReplicaServer(i, "./");
                
                ReplicaInterface stub = (ReplicaInterface) UnicastRemoteObject.exportObject(rs, 0);
                registry.rebind("ReplicaClient" + i, stub);
                
                master.registerReplicaServer(replicaLoc, stub);
                
                System.out.println("replica server state [@ main] = " + rs.isAlive());
            }
        }
    }

    @SuppressWarnings("empty-statement")
    public static void launchClients() {
        try {
            Client c = new Client();
            char[] ss = "File 1 test test END \n".toCharArray();
            byte[] data = new byte[ss.length];
            for (int i = 0; i < ss.length; i++) {
                data[i] = (byte) ss[i];
            }

            c.write("file1", data);
            byte[] ret = c.read("file1");
            System.out.println("file1: " + Arrays.toString(ret));

            c = new Client();
            ss = "File 1 Again Again END\n ".toCharArray();
            data = new byte[ss.length];
            for (int i = 0; i < ss.length; i++) {
                data[i] = (byte) ss[i];
            }

            c.write("file1", data);
            ret = c.read("file1");
            System.out.println("file1: " + Arrays.toString(ret));

            c = new Client();
            ss = "File 2 test test END \n".toCharArray();
            data = new byte[ss.length];
            for (int i = 0; i < ss.length; i++) {
                data[i] = (byte) ss[i];
            }

            c.write("file2", data);
            ret = c.read("file2");
            System.out.println("file2: " + Arrays.toString(ret));
            while (true);

        } catch (NotBoundException | IOException | MessageNotFoundException e) {

        }
    }
    
    // menu for user console
    public static void showMenu() {
        System.out.println(" insert dir to see files inside folder");
        System.out.println(" insert read to read a file from the folder");
        System.out.println(" insert write to write something in a file");
        System.out.println(" insert new to add a new client in server");
        System.out.println(" insert exit to close the program");

    }
    
    // read data from user input
    public static String readFromConsole() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String input = null;
        try {
            input = reader.readLine();
        } catch (IOException e) {
            System.err.println("Cannot read from console");
        }
        return input;

    }
    
    // display files from directory
    public static void dir() {
        System.out.println("file1");
        System.out.println("file2");
    }

    // read the file
    public static void read(Client c) {
        System.out.println("insert file name");
        String file = readFromConsole();
        if(!"file1".equals(file) && !"file2".equals(file)){
            System.err.println("Cannot read file " + file);
        }
        else{
            wFile(c, file, " ");  // to init file for read 

            try {
                byte[] ret = c.read(file);
                System.out.println("file1: " + Arrays.toString(ret));
            } catch (IOException | NotBoundException e) {
            }
        }
    }

    // write data to the file
    public static void wFile(Client c, String fileN, String text) {
        char[] ss = text.toCharArray();
        byte[] data = new byte[ss.length];
        for (int i = 0; i < ss.length; i++) {
            data[i] = (byte) ss[i];
        }
        try {
            c.write(fileN, data);
        } catch (IOException | NotBoundException | MessageNotFoundException e) {
        }

    }
    
    // get data from user and init write it on file 
    public static void write(Client c) {
        System.out.println("insert file name");
        String file = readFromConsole();
        if(!"file1".equals(file) && !"file2".equals(file)){
            System.err.println("Cannot write file " + file);
        }
        else{
            System.out.println("insert the text to be put in the file");
            String text = readFromConsole();
            wFile(c, file, text);
        }
    }
  
    // client/user console controler
    public static void clientLaunch() {
        Client client = new Client(); 
        System.out.println("new client is online");
        while (true) { 
            showMenu();
            switch (readFromConsole()) {
                case "dir":
                    dir();
                    break;
                case "read":
                    read(client);
                    break;
                case "write":
                    write(client);
                    break;
                case "new":
                    clientLaunch();
                    break;
                case "exit":
                    exit(0);
                    break;
            }
        }
    }

    /**
     * runs a custom test as follows 1. write initial text to "file1" 2. reads
     * the recently text written to "file1" 3. writes a new message to "file1"
     * 4. while the writing operation in progress read the content of "file1" 5.
     * the read content should be = to the initial message 6. commit the 2nd
     * write operation 7. read the content of "file1", should be = initial
     * messages then second message
     *
     * @throws IOException
     * @throws NotBoundException
     * @throws MessageNotFoundException
     */
    public static void customTest() throws IOException, NotBoundException, MessageNotFoundException {
        Client c = new Client();
        String fileName = "file1";

        char[] ss = "[INITIAL DATA!] ".toCharArray(); // len = 15
        byte[] data = new byte[ss.length];
        for (int i = 0; i < ss.length; i++) {
            data[i] = (byte) ss[i];
        }

        c.write(fileName, data);

        c = new Client();
        ss = "File 1 test test END ".toCharArray(); // len = 20
        data = new byte[ss.length];
        for (int i = 0; i < ss.length; i++) {
            data[i] = (byte) ss[i];
        }

        byte[] chunk = new byte[Configurations.CHUNK_SIZE];

        int seqN = data.length / Configurations.CHUNK_SIZE;
        int lastChunkLen = Configurations.CHUNK_SIZE;

        if (data.length % Configurations.CHUNK_SIZE > 0) {
            lastChunkLen = data.length % Configurations.CHUNK_SIZE;
            seqN++;
        }

        WriteAck ackMsg = c.masterStub.write(fileName);
        ReplicaServerClientInterface stub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient" + ackMsg.getLoc().getId());

        FileContent fileContent;
        @SuppressWarnings("unused")
        ChunkAck chunkAck;
        //		for (int i = 0; i < seqN; i++) {
        System.arraycopy(data, 0 * Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
        fileContent = new FileContent(fileName, chunk);
        chunkAck = stub.write(ackMsg.getTransactionId(), 0, fileContent);

        System.arraycopy(data, 1 * Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
        fileContent = new FileContent(fileName, chunk);
        chunkAck = stub.write(ackMsg.getTransactionId(), 1, fileContent);

        // read here 
        List<ReplicaLoc> locations = c.masterStub.read(fileName);
        System.err.println("[@CustomTest] Read1 started ");

        // TODO fetch from all and verify 
        ReplicaLoc replicaLoc = locations.get(0);
        ReplicaServerClientInterface replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient" + replicaLoc.getId());
        fileContent = replicaStub.read(fileName);
        System.err.println("[@CustomTest] data:");
        System.err.println(new String(fileContent.getData()));

        // continue write 
        for (int i = 2; i < seqN - 1; i++) {
            System.arraycopy(data, i * Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
            fileContent = new FileContent(fileName, chunk);
            chunkAck = stub.write(ackMsg.getTransactionId(), i, fileContent);
        }
        // copy the last chuck that might be < CHUNK_SIZE
        System.arraycopy(data, (seqN - 1) * Configurations.CHUNK_SIZE, chunk, 0, lastChunkLen);
        fileContent = new FileContent(fileName, chunk);
        chunkAck = stub.write(ackMsg.getTransactionId(), seqN - 1, fileContent);

        //commit
        ReplicaLoc primaryLoc = c.masterStub.locatePrimaryReplica(fileName);
        ReplicaServerClientInterface primaryStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient" + primaryLoc.getId());
        primaryStub.commit(ackMsg.getTransactionId(), seqN);

        // read
        locations = c.masterStub.read(fileName);
        System.err.println("[@CustomTest] Read2 started ");

        replicaLoc = locations.get(0);
        replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient" + replicaLoc.getId());
        fileContent = replicaStub.read(fileName);
        System.err.println("[@CustomTest] data:");
        System.err.println(new String(fileContent.getData()));

    }

    static Master startMaster() throws AccessException, RemoteException {
        Master master = new Master();
        MasterServerClientInterface stub = (MasterServerClientInterface) UnicastRemoteObject.exportObject(master, 0);
        registry.rebind("MasterServerClientInterface", stub);
        System.err.println("Server ready");
        return master;
    }

    public static void main(String[] args) throws IOException, NotBoundException, MessageNotFoundException {

        try {
            LocateRegistry.createRegistry(regPort);
            registry = LocateRegistry.getRegistry(regPort);

            Master master = startMaster();
            respawnReplicaServers(master);
            
            customTest();
            clientLaunch();

        } catch (RemoteException e) {

            System.err.println("Server on port " + regPort + " already in use");
            System.err.println("new server is not starting");

        }
    }

}
