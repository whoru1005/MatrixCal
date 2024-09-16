import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MasterNode {
    private static final int PORT = 50000;  // 서버 포트 번호
    private static final int MATRIX_SIZE = 1000;  // 행렬 크기 (1000x1000)
    private static final int MAX_CLIENTS = 4;  // 최대 클라이언트 수
    private static int[][] matrixA = new int[MATRIX_SIZE][MATRIX_SIZE];  // 행렬 A
    private static int[][] matrixB = new int[MATRIX_SIZE][MATRIX_SIZE];  // 행렬 B
    private static int[][] resultMatrix = new int[MATRIX_SIZE][MATRIX_SIZE];  // 결과 행렬
    private static AtomicInteger clientCount = new AtomicInteger(0);  // 연결된 클라이언트 수를 관리하는 원자 변수
    private static List<Socket> connectedClients = new ArrayList<>();  // 연결된 클라이언트 소켓을 저장하는 리스트
    private static BufferedWriter logWriter;  // 로그 파일 작성을 위한 BufferedWriter

    public static void main(String[] args) {
        try {
            // 현재 시간을 기반으로 로그 파일 생성
            String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
            String logFileName = "MasterNode_" + timeStamp + ".txt";
            logWriter = new BufferedWriter(new FileWriter(logFileName, true));  // 로그 파일 쓰기 모드로 열기

            logMessage("서버가 시작되었습니다.");  // 로그 메시지 기록

            // 서버 소켓을 열어 클라이언트 연결 대기
            try (ServerSocket serverSocket = new ServerSocket(PORT)) {
                logMessage("Server is running on port " + PORT);  // 서버 실행 로그

                generateMatrices();  // 행렬 A와 B 생성
                logMessage("행렬 A와 B가 생성되었습니다.");  // 행렬 생성 완료 로그

                // 4개의 클라이언트가 모두 연결될 때까지 대기
                while (clientCount.get() < MAX_CLIENTS) {
                    Socket clientSocket = serverSocket.accept();  // 클라이언트 연결 수락
                    connectedClients.add(clientSocket);  // 연결된 클라이언트 소켓 저장
                    logMessage(clientCount.incrementAndGet() + "번째 클라이언트가 연결되었습니다.");  // 클라이언트 연결 로그
                }

                logMessage("모든 클라이언트가 연결되었습니다.");  // 4개의 클라이언트 모두 연결 완료 로그

                // 모든 클라이언트 연결 후 작업 분배
                distributeTasksToClients();  // Task 생성 후 작업을 클라이언트에게 분배
            }
        } catch (IOException e) {
            e.printStackTrace();  // 예외 발생 시 스택 트레이스 출력
        } finally {
            // 프로그램 종료 시 로그 파일 닫기
            try {
                if (logWriter != null) {
                    logWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // 행렬 A와 B를 랜덤 값으로 생성하는 메서드
    private static void generateMatrices() {
        Random rand = new Random();  // 랜덤 숫자 생성기
        for (int i = 0; i < MATRIX_SIZE; i++) {
            for (int j = 0; j < MATRIX_SIZE; j++) {
                matrixA[i][j] = rand.nextInt(99) + 1;  // 1~99 사이의 값으로 행렬 A 초기화
                matrixB[i][j] = rand.nextInt(99) + 1;  // 1~99 사이의 값으로 행렬 B 초기화
            }
        }
    }

    // 모든 클라이언트에게 작업을 분배하는 메서드
    private static void distributeTasksToClients() throws IOException {
        // 각 클라이언트에 대해 작업을 생성하고 전송
        for (int clientNumber = 1; clientNumber <= MAX_CLIENTS; clientNumber++) {
            // 각 클라이언트에 할당할 Task 리스트 생성
            List<Task> taskList = generateTasksForClient(clientNumber);

            // 해당 클라이언트로 Task 객체 전송
            Socket clientSocket = connectedClients.get(clientNumber - 1);  // 연결된 클라이언트 소켓 가져오기
            ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());  // 클라이언트로 데이터 전송을 위한 스트림 생성
            out.writeObject(taskList);  // 생성한 Task 리스트를 클라이언트로 전송
            out.flush();  // 스트림 플러시

            logMessage("클라이언트 " + clientNumber + "에게 작업을 전송했습니다.");  // 작업 전송 완료 로그
        }
    }

    // 각 클라이언트에 할당할 Task 리스트를 생성하는 메서드
    private static List<Task> generateTasksForClient(int clientNumber) {
        List<Task> taskList = new ArrayList<>();  // Task 객체를 담을 리스트 생성
        int startRow = (clientNumber - 1) * (MATRIX_SIZE / MAX_CLIENTS);  // 클라이언트에 할당할 행렬의 시작 행
        int endRow = clientNumber * (MATRIX_SIZE / MAX_CLIENTS);  // 클라이언트에 할당할 행렬의 마지막 행

        // 할당된 행렬의 범위 내에서 Task 생성
        for (int i = startRow; i < endRow; i++) {
            for (int j = 0; j < MATRIX_SIZE; j++) {
                // Task 객체 생성 (행렬 A의 i번째 행과 행렬 B의 j번째 열을 기반으로 생성)
                Task task = new Task(matrixA[i], getColumn(matrixB, j), i, j);
                taskList.add(task);  // 생성된 Task를 리스트에 추가
            }
        }

        return taskList;  // 생성된 Task 리스트 반환
    }

    // 행렬 B의 열을 추출하는 메서드
    private static int[] getColumn(int[][] matrix, int columnIndex) {
        int[] column = new int[MATRIX_SIZE];  // 행렬의 열을 담을 배열
        for (int i = 0; i < MATRIX_SIZE; i++) {
            column[i] = matrix[i][columnIndex];  // 행렬 B의 columnIndex 열을 추출
        }
        return column;  // 열 배열 반환
    }

    // 로그 메시지를 기록하는 메서드
    private static void logMessage(String message) throws IOException {
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());  // 현재 시간
        String logMessage = timeStamp + " - " + message;  // 로그 메시지에 시간 포함
        System.out.println(logMessage);  // 콘솔에 출력
        logWriter.write(logMessage);  // 로그 파일에 기록
        logWriter.newLine();  // 새 줄 추가
        logWriter.flush();  // 스트림 플러시
    }
}
