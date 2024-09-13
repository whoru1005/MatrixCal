import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class MasterNode {
    private static final int PORT = 50000;
    private static int clientCnt = 0;
    private static Timer timer;
    private WorkerNode[] workers;
    private static LogWriter logWriter;

    public static void main(String[] args) throws IOException {

        timer = new Timer();
        logWriter = new LogWriter("MasterNode", timer);
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            while (true) {
                // 클라이언트 접속 대기
                Socket clientSocket = serverSocket.accept();
                logTime("클라이언트가 접속하였습니다.");
                // 클라이언트 요청을 새로운 스레드로 처리
                new Thread(new ClientHandler(clientSocket)).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 클라이언트 요청을 처리하는 스레드
    private static class ClientHandler implements Runnable {
        private Socket clientSocket;

        public ClientHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try {
                // 클라이언트와 통신 설정
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

                // 서버 시스템 클락 기준의 경과 시간 전송
                long currentTime = systemClock.getCurrentTime();
                out.write("서버 시간: " + currentTime + "ms\n");
                out.flush();

                // 클라이언트 메시지 수신
                String clientMessage = in.readLine();
                logTime("클라이언트 메시지: " + clientMessage);

                // 서버 응답
                out.write("서버 응답: 접속이 완료되었습니다.\n");
                out.flush();

                clientSocket.close();  // 클라이언트 연결 종료
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}

