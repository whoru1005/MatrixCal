import java.io.*;
import java.net.Socket;

public class WorkerNode implements Runnable {
    private static final String SERVER_IP = "127.0.0.1";  // GCP의 경우 외부 IP로 변경
    private static final int PORT = 50000;

    public WorkerNode(int id) {
        // 클라이언트 ID를 설정할 수 있지만 여기서는 필요하지 않음
    }

    public static void main(String[] args) {
        // 워커 노드가 실행될 때마다 스레드를 통해 접속
        WorkerNode worker = new WorkerNode(1);
        Thread workerThread = new Thread(worker);
        workerThread.start();
    }

    @Override
    public void run() {
        try {
            Socket socket = new Socket(SERVER_IP, PORT);

            // 서버로부터 시스템 클락 시간을 수신
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String serverTime = in.readLine();  // 서버에서 보내준 시간
            logTime("서버로부터 받은 시간: " + serverTime);

            // 서버에 클라이언트 메시지 전송
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            out.write("WorkerNode가 접속하였습니다.\n");
            out.flush();

            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 서버에서 받은 시간 정보를 이용해 로깅하는 메서드
    private static void logTime(String message) {
        System.out.println(message);
    }
}
