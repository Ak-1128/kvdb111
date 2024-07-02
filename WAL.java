package common;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class WAL {
    private BufferedWriter writer;
    private String logFileName;

    //构造方法
    public WAL(String logFileName) throws IOException {
        this.logFileName = logFileName;
        writer = new BufferedWriter(new FileWriter(logFileName, true));
    }


    //日志记录
    public void log(String entry) throws IOException {
        // Write the entry without extra new lines
        writer.write(entry.trim());
        writer.newLine();  // Ensure it's written to disk
        writer.flush();
    }
//关闭流
    public void close() throws IOException {
        writer.close();
    }

    public String getLogFileName() {
        return logFileName;
    }
}
