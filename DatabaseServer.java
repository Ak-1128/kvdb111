package server;

import common.Database;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class DatabaseServer {
    private static final int PORT = 12345;
    private Database database;

    //服务器启动
    public DatabaseServer(String logFileName, String initialFileName) throws IOException {
        database = new Database(logFileName, initialFileName);
    }

    public void start() {//接受连接请求
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Server started on port " + PORT);

            while (true) {
                try (Socket clientSocket = serverSocket.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                     PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

                    String request;
                    while ((request = in.readLine()) != null) {
                        String response = handleRequest(request);
                        out.println(response);
                    }
                } catch (IOException e) {
                    System.err.println("Client handling error: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
    }

    private String handleRequest(String request) {
        String[] parts = request.split(" ", 3);
        if (parts.length < 2) {
            return "Invalid command";
        }

        String action = parts[0].toUpperCase();
        String key = parts[1];
//接收操作命令
        try {
            switch (action) {
                case "SET":
                    if (parts.length != 3) {
                        return "Invalid SET command";
                    }
                    database.set(key, parts[2]);
                    return "Key " + key + " set";

                case "GET":
                    String value = database.get(key);
                    return value != null ? "Value: " + value : "Key not found";

                case "RM":
                    database.rm(key);
                    return "Key " + key + " RM";

                default:
                    return "Unknown command";
            }
        } catch (IOException e) {
            return "Error: " + e.getMessage();
        }
    }

    public static void main(String[] args) {
        try {
            DatabaseServer server = new DatabaseServer("wal.log", "data.log");
            server.start();
        } catch (IOException e) {
            System.err.println("Server initialization error: " + e.getMessage());
        }
    }
}
