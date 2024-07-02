package common;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

public class FileManager {
    private static final int MAX_FILE_SIZE = 1 * 1024; // 1KB for testing, adjust as needed
    private static final ExecutorService executor = Executors.newSingleThreadExecutor();

    private File currentFile;
    private BufferedWriter writer;
    private int fileIndex = 0; // Tracks the number of rotated files
    private Map<String, String> index; // Key to file name and position map

    //构造方法
    public FileManager(String initialFileName) throws IOException {
        currentFile = new File(initialFileName);
        writer = new BufferedWriter(new FileWriter(currentFile, true));
        index = new HashMap<>();
        // loadIndex(); // Load index on initialization
    }

    //写入文件
    public synchronized void write(String key, String value) throws IOException {
        if (currentFile.length() > MAX_FILE_SIZE) {
            rotateFile();
        }
        long position = currentFile.length();
        String data = key + " " + value;
        writer.write(data);
        writer.newLine();//换行
        writer.flush();
        //写入文件刷新索引
        index.put(key, currentFile.getName() + ":" + position); // Update index
    }

    public synchronized String read(String key) throws IOException {
        if (!index.containsKey(key)) {
            return null; // Key not found
        }
        String[] fileInfo = index.get(key).split(":");
        String fileName = fileInfo[0];
        long position = Long.parseLong(fileInfo[1]);

        try (RandomAccessFile file = new RandomAccessFile(fileName, "r")) {
            file.seek(position);
            return file.readLine();
        }
    }

    //rotate
    private synchronized void rotateFile() throws IOException {
        System.out.println("Rotating file: " + currentFile.getAbsolutePath());
        writer.close(); // Close the current file writer
        File oldFile = currentFile;
        fileIndex++; // Increment the file index for new file name
        executor.submit(() -> compressFile(oldFile)); // Compress the old file in a separate thread

        currentFile = new File("data_" + fileIndex + ".log"); // Create a new file with the incremented index
        writer = new BufferedWriter(new FileWriter(currentFile, true)); // Open a new file writer
    }

    //压缩
    private void compressFile(File file) {
        System.out.println("Compressing file: " + file.getAbsolutePath());
        try (FileInputStream fis = new FileInputStream(file);
             FileOutputStream fos = new FileOutputStream(file.getPath() + ".gz");
             GZIPOutputStream gzipOS = new GZIPOutputStream(fos)) {
            byte[] buffer = new byte[1024];
            int length;
            while ((length = fis.read(buffer)) > 0) {
                gzipOS.write(buffer, 0, length);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (!file.delete()) { // Delete the original file after compression
                System.err.println("Failed to delete the original file: " + file.getAbsolutePath());
            } else {
                System.out.println("Deleted original file: " + file.getAbsolutePath());
            }
        }
    }

    //保存索引
    public void close() throws IOException {
        writer.close();
        executor.shutdown();
        saveIndex(); // Save index to disk on close
    }

    private void saveIndex() throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("index.dat"))) {
            oos.writeObject(index);
        }
    }
}

