package common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class Database {
    private ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    private FileManager fileManager;
    private WAL wal;

    //构造方法
    public Database(String logFileName, String initialFileName) throws IOException {
        fileManager = new FileManager(initialFileName);
        wal = new WAL(logFileName);
        replayLog(); //回放
    }

    public void set(String key, String value) throws IOException {
        store.put(key, value);
        fileManager.write(key, value); // Use the correct signature
        wal.log("SET " + key + " " + value + "\n");
    }

    public String get(String key) {
        return store.get(key);
    }

    public void rm(String key) throws IOException {
        store.remove(key);
        fileManager.write(key, ""); // Indicate deletion with an empty value
        wal.log("RM " + key + "\n");
    }

    public void close() throws IOException {
        fileManager.close();
        wal.close();
    }


    //回放
    private void replayLog() throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(wal.getLogFileName()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                String command = parts[0];
                String key = parts[1];
                if (command.equals("SET")) {
                    String value = parts[2];
                    store.put(key, value);
                    fileManager.write(key, value); // Ensure the data is written to the main file
                } else if (command.equals("RM")) {
                    store.remove(key);
                    fileManager.write(key, ""); // Ensure the data is written to the main file
                }
            }
        }
    }
}
