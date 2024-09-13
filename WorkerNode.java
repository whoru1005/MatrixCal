import java.io.*;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;

public class WorkerNode {
    private static final String SERVER_IP = "34.64.154.49";  // 서버 IP, GCP의 경우 외부 IP를 입력
    private static final int PORT = 50000;
    private static BufferedWriter logWriter;
    private static int clientNumber;

    public static void main(String[] args) {
        try {
            // 서버에 접속 시도
            logMessage("서버에 접속을 요청하였습니다.");

            // 서버에 접속
            Socket socket = new Socket(SERVER_IP, PORT);

            // 서버로부터 클라이언트 고유 번호를 전달받음
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            clientNumber = Integer.parseInt(in.readLine());

            // 현재 시간을 기준으로 로그 파일 생성 (고유 번호 포함)
            String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
            String logFileName = "WorkerNode_" + clientNumber + "_" + timeStamp + ".txt";
            logWriter = new BufferedWriter(new FileWriter(logFileName, true));

            // 접속 완료 시 로그 생성
            logMessage("서버에 접속을 완료하였습니다. 할당된 번호: " + clientNumber);

            // 서버와 간단한 통신 메시지 확인 후 종료
            socket.close();  // 연결 종료
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

    // 로그를 남기는 메서드 (현재 시간 포함)
    private static void logMessage(String message) throws IOException {
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        String logMessage = timeStamp + " - " + message;
        System.out.println(logMessage);  // 터미널에 출력
        if (logWriter != null) {
            logWriter.write(logMessage);
            logWriter.newLine();
            logWriter.flush();
        }
    }
}
