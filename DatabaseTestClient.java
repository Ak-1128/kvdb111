package client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class DatabaseTestClient {
    private static final String SERVER_ADDRESS = "localhost";
    private static final int SERVER_PORT = 12345;

    public static void main(String[] args) {
        try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Number of SET commands to send
            int numberOfCommands = 1001; // Adjust as needed to trigger file rotation
            for (int i = 800; i < numberOfCommands; i++) {
                String key = "czk" + i;
                String value = "v" + i;
                String command = "SET " + key + " " + value;
                out.println(command);

                // Read the server response
                String response = in.readLine();
                System.out.println("Server response: " + response);

                // Optional: Add a short delay to avoid overwhelming the server
                Thread.sleep(10);
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Client error: " + e.getMessage());
        }
    }
}
