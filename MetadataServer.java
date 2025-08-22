import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

// MetadataServer acts as the master node in the DFS. It stores metadata about files
// and their chunk locations, but not the actual file data.
public class MetadataServer {

    private static final int PORT = 9000; // Port for clients to connect to the Metadata Server.
    // Maps filename to a list of ChunkInfo (which Data Server holds which chunk).
    private static Map<String, List<ChunkInfo>> fileToChunksMap = Collections.synchronizedMap(new HashMap<>());
    // List of active Data Server addresses. In a real system, this would be dynamically managed.
    private static List<String> dataServerAddresses = Collections.synchronizedList(new ArrayList<>());
    private static int nextDataServerIndex = 0; // For simple round-robin load balancing.

    public static void main(String[] args) {
        System.out.println("Metadata Server started on port " + PORT);

        // Add some dummy Data Server addresses for demonstration.
        // In a real system, Data Servers would register themselves.
        dataServerAddresses.add("localhost:9001");
        dataServerAddresses.add("localhost:9002");
        dataServerAddresses.add("localhost:9003");

        ExecutorService clientPool = Executors.newFixedThreadPool(10); // Thread pool for client connections.

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected: " + clientSocket);
                clientPool.execute(new ClientHandler(clientSocket));
            }
        } catch (IOException e) {
            System.err.println("Metadata Server error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            clientPool.shutdown();
        }
    }

    // Assigns chunks to Data Servers for a new file.
    // A real system would consider replication and more sophisticated load balancing.
    public static List<ChunkInfo> assignChunks(String filename, int numberOfChunks) {
        List<ChunkInfo> chunkLocations = new ArrayList<>();
        if (dataServerAddresses.isEmpty()) {
            System.err.println("No Data Servers available to assign chunks.");
            return Collections.emptyList();
        }

        // Simple round-robin assignment and 3x replication
        for (int i = 0; i < numberOfChunks; i++) {
            String chunkId = filename + "_chunk_" + i;
            // For simplicity, we'll assign to the next 3 available data servers
            // A more robust system would ensure these are distinct servers.
            String server1 = dataServerAddresses.get(nextDataServerIndex++ % dataServerAddresses.size());
            String server2 = dataServerAddresses.get(nextDataServerIndex++ % dataServerAddresses.size());
            String server3 = dataServerAddresses.get(nextDataServerIndex++ % dataServerAddresses.size());

            // For now, we'll just store one location. Replication logic is more complex.
            chunkLocations.add(new ChunkInfo(chunkId, server1));
            System.out.printf("Assigned chunk %s to %s%n", chunkId, server1);
        }
        fileToChunksMap.put(filename, chunkLocations);
        return chunkLocations;
    }

    // Retrieves chunk locations for a given file.
    public static List<ChunkInfo> getChunkLocations(String filename) {
        return fileToChunksMap.getOrDefault(filename, Collections.emptyList());
    }

    // ClientHandler manages communication with a connected client.
    private static class ClientHandler implements Runnable {
        private Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try (ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream())) {

                String command = (String) in.readObject(); // Read command from client.
                String filename = (String) in.readObject(); // Read filename.

                System.out.println("Received command: " + command + " for file: " + filename);

                switch (command) {
                    case "UPLOAD_REQUEST":
                        int numberOfChunks = (int) in.readObject();
                        List<ChunkInfo> locationsForUpload = assignChunks(filename, numberOfChunks);
                        out.writeObject(locationsForUpload); // Send chunk locations to client.
                        out.flush();
                        break;
                    case "DOWNLOAD_REQUEST":
                        List<ChunkInfo> locationsForDownload = getChunkLocations(filename);
                        out.writeObject(locationsForDownload); // Send chunk locations to client.
                        out.flush();
                        break;
                    default:
                        out.writeObject(Collections.emptyList()); // Unknown command.
                        out.flush();
                }

            } catch (IOException | ClassNotFoundException e) {
                System.err.println("Error in ClientHandler: " + e.getMessage());
                // e.printStackTrace(); // For debugging
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    System.err.println("Error closing client socket: " + e.getMessage());
                }
            }
        }
    }

    // Helper class to store chunk information.
    static class ChunkInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        String chunkId;
        String dataServerAddress; // e.g., "localhost:9001"

        public ChunkInfo(String chunkId, String dataServerAddress) {
            this.chunkId = chunkId;
            this.dataServerAddress = dataServerAddress;
        }

        public String getChunkId() { return chunkId; }
        public String getDataServerAddress() { return dataServerAddress; }

        @Override
        public String toString() {
            return "ChunkInfo{" +
                   "chunkId='" + chunkId + '\'' +
                   ", dataServerAddress='" + dataServerAddress + '\'' +
                   '}';
        }
    }
}
