import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

public class MasterNode {
    private static final int PORT = 50000;
    private static AtomicInteger clientCount = new AtomicInteger(0);
    private static BufferedWriter logWriter;

    public static void main(String[] args) {
        try {
            // 현재 시간을 기준으로 로그 파일 생성
            String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
            String logFileName = "MasterNode_" + timeStamp + ".txt";
            logWriter = new BufferedWriter(new FileWriter(logFileName, true)); // 기존 파일에 추가

            logMessage("서버가 시작되었습니다.");

            try (ServerSocket serverSocket = new ServerSocket(PORT)) {
                System.out.println("Server is running on port " + PORT);

                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    new Thread(new ClientHandler(clientSocket)).start();  // 클라이언트 요청을 처리하는 스레드 생성
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (logWriter != null) {
                    logWriter.close();  // 프로그램 종료 시 로그 파일 닫기
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // 클라이언트 요청을 처리하는 스레드
    private static class ClientHandler implements Runnable {
        private Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try {
                int clientNumber = clientCount.incrementAndGet();  // 클라이언트 고유 번호 할당
                logMessage(clientNumber + "번째 클라이언트가 접속하였습니다.");

                // 4번째 클라이언트가 접속하면 모든 클라이언트 접속 메시지 기록
                if (clientNumber == 4) {
                    logMessage("모든 클라이언트가 접속했습니다.");
                }

                // 클라이언트에게 고유 번호 전달
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
                out.write(String.valueOf(clientNumber));
                out.newLine();
                out.flush();

                clientSocket.close();  // 연결 종료
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // 로그를 남기는 메서드 (현재 시간 포함)
    private static void logMessage(String message) throws IOException {
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        String logMessage = timeStamp + " - " + message;
        System.out.println(logMessage);  // 터미널에 출력
        logWriter.write(logMessage);
        logWriter.newLine();
        logWriter.flush();
    }
}
