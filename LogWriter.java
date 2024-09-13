import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

class LogWriter {
    private Timer timer;
    private String fileName;
    private BufferedWriter logWriter;

    public LogWriter(String filename, Timer timer) throws IOException {
        this.timer = timer;
        this.fileName = filename + ".txt";
        this.logWriter = new BufferedWriter(new FileWriter(fileName, true));
    }

    public void LoggingMsg(String msg) throws IOException {
        String logMsg = "[" + timer + "] " + msg ;
        System.out.println(logMsg);  // 터미널에 출력
        if (logWriter != null) {
            logWriter.write(logMsg);
            logWriter.newLine();
            logWriter.flush();
        }
    }

}

