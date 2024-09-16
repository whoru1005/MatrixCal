import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class WorkerNode {
    private static final int MAX_QUEUE_SIZE = 10;  // 최대 작업 큐 크기
    private Queue<Task> taskQueue = new LinkedList<>();  // 작업 큐
    private BufferedReader in;
    private BufferedWriter out;
    private LogWriter logWriter;
    private ExecutorService calculationExecutor;  // 계산 작업을 위한 스레드 풀
    private ScheduledExecutorService statusCheckExecutor;  // WorkerNode 상태를 체크하는 스레드
    private ExecutorService serverCommunicationExecutor;  // 서버 통신을 위한 스레드 풀
    private List<WorkerNode> otherWorkerNodes;  // 다른 WorkerNode와의 통신 리스트
    private Socket socket;  // 서버와의 통신 소켓

    public WorkerNode(Socket socket) {
        try {
            this.socket = socket;
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));  // 소켓 입력 스트림
            out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));  // 소켓 출력 스트림
        } catch (IOException e) {
            e.printStackTrace();
        }

        logWriter = new LogWriter("WorkerNode");
        calculationExecutor = Executors.newSingleThreadExecutor();  // 계산 작업을 처리하는 스레드
        statusCheckExecutor = Executors.newScheduledThreadPool(1);  // 다른 WorkerNode와 통신하는 스레드
        serverCommunicationExecutor = Executors.newSingleThreadExecutor();  // 서버와 통신하는 스레드
        otherWorkerNodes = new ArrayList<>();  // 다른 WorkerNode 리스트

        // 서버와 통신하는 쓰레드를 시작
        serverCommunicationExecutor.submit(this::startServerCommunication);

        // 다른 WorkerNode와의 통신을 시작
        startWorkerNodeCommunication();
    }

    // 서버와 통신하는 작업 (쓰레드 1)
    private void startServerCommunication() {
        try {
            while (true) {
                String serverMessage = in.readLine();  // 서버로부터 메시지 수신
                if (serverMessage != null) {
                    logWriter.writeLog("서버로부터 작업 수신: " + serverMessage);
                    Task task = parseTask(serverMessage);  // 수신된 메시지를 작업으로 변환
                    receiveTask(task);  // 작업 큐에 추가
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 작업을 큐에 추가하고 계산을 시작 (쓰레드 2)
    public void receiveTask(Task task) {
        if (taskQueue.size() < MAX_QUEUE_SIZE) {
            taskQueue.add(task);  // 작업 큐에 추가
            logWriter.writeLog("작업이 큐에 추가되었습니다: 행 " + task.getRowIndex() + ", 열 " + task.getColumnIndex());
            processNextTask();  // 계산 작업 처리
        } else {
            logWriter.writeLog("작업 큐가 가득 찼습니다. 작업 할당을 거부합니다.");
            sendFailureToMaster(task);  // 작업 실패 보고
        }
    }

    // 계산 작업 처리 (쓰레드 2)
    private void processNextTask() {
        calculationExecutor.submit(() -> {
            while (!taskQueue.isEmpty()) {
                Task task = taskQueue.poll();  // 큐에서 작업 꺼냄
                logWriter.writeLog("작업을 처리 중입니다: 행 " + task.getRowIndex() + ", 열 " + task.getColumnIndex());
                performComputation(task);  // 작업 수행
            }
        });
    }

    // 작업을 처리하는 로직
    private void performComputation(Task task) {
        Timer.delayComputation();  // 1~3초 연산 지연 시뮬레이션

        // A의 한 행과 B의 한 열을 곱한 결과 계산
        int result = 0;
        for (int i = 0; i < task.getMatrixARow().length; i++) {
            result += task.getMatrixARow()[i] * task.getMatrixBColumn()[i];
        }

        // 연산 성공 또는 실패 처리
        if (randomSuccess()) {
            sendResult(task, result);  // 성공 시 결과 전송
            logWriter.writeLog("작업 성공: 행 " + task.getRowIndex() + ", 열 " + task.getColumnIndex());
        } else {
            sendFailureToMaster(task);  // 실패 처리
            logWriter.writeLog("작업 실패: 행 " + task.getRowIndex() + ", 열 " + task.getColumnIndex());
        }
    }

    // 작업 성공 여부 결정 (80% 성공 확률)
    private boolean randomSuccess() {
        return new Random().nextInt(100) < 80;  // 80% 확률로 성공
    }

    // 작업 결과를 MasterNode로 전송
    private void sendResult(Task task, int result) {
        try {
            out.write("RESULT:" + task.getRowIndex() + "," + task.getColumnIndex() + "," + result + "\n");
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 작업 실패를 MasterNode로 보고
    private void sendFailureToMaster(Task task) {
        try {
            out.write("FAILURE:" + task.getRowIndex() + "," + task.getColumnIndex() + "\n");
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 다른 WorkerNode들과의 통신을 담당하는 스레드 (쓰레드 3)
    public void startWorkerNodeCommunication() {
        statusCheckExecutor.scheduleAtFixedRate(() -> {
            logWriter.writeLog("WorkerNode 상태 확인 중...");
            broadcastStatus();  // 상태 정보 브로드캐스트
            checkQueueAndReassign();  // 작업 재할당 여부 확인
        }, 0, 5, TimeUnit.SECONDS);  // 5초 간격으로 상태 체크 및 작업 재할당
    }

    // 작업 큐가 가득 찼을 때 다른 WorkerNode로 재할당 시도
    private void checkQueueAndReassign() {
        if (taskQueue.size() >= MAX_QUEUE_SIZE) {
            Task task = taskQueue.poll();  // 대기 중인 작업 하나를 꺼냄
            reassignTaskToLeastBusyNode(task);  // 재할당
        }
    }

    // 작업을 다른 WorkerNode로 재할당
    private void reassignTaskToLeastBusyNode(Task task) {
        WorkerNodeInterface leastBusyNode = findLeastBusyNode();
        if (leastBusyNode != null && leastBusyNode.getQueueSize() < MAX_QUEUE_SIZE) {
            leastBusyNode.receiveTask(task);  // 작업 재할당
            logWriter.writeLog("작업을 다른 WorkerNode로 재할당: 행 " + task.getRowIndex() + ", 열 " + task.getColumnIndex());
        } else {
            logWriter.writeLog("작업 재할당 실패: 모든 노드가 가득 참.");
        }
    }

    // 가장 적은 작업을 가진 WorkerNode를 찾는 메서드
    private WorkerNodeInterface findLeastBusyNode() {
        return otherWorkerNodes.stream()
                .min(Comparator.comparingInt(WorkerNodeInterface::getQueueSize))
                .orElse(null);
    }

    // WorkerNode의 상태를 다른 노드들에게 브로드캐스트
    private void broadcastStatus() {
        String statusMessage = "WorkerNode: " + getQueueSize() + " 작업 대기";
        for (WorkerNodeInterface workerNode : otherWorkerNodes) {
            try {
                Socket workerSocket = new Socket(workerNode.getHost(), workerNode.getPort());
                BufferedWriter workerOut = new BufferedWriter(new OutputStreamWriter(workerSocket.getOutputStream()));
                workerOut.write(statusMessage + "\n");
                workerOut.flush();
                workerOut.close();
                workerSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // 현재 WorkerNode의 작업 큐 크기 반환
    public int getQueueSize() {
        return taskQueue.size();
    }

    // 종료 시 스레드 풀 종료
    public void shutdownWorkerNode() {
        try {
            calculationExecutor.shutdown();
            if (!calculationExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                calculationExecutor.shutdownNow();
            }
            statusCheckExecutor.shutdown();
            serverCommunicationExecutor.shutdown();
        } catch (InterruptedException e) {
            calculationExecutor.shutdownNow();
        }
        logWriter.writeLog("WorkerNode가 종료되었습니다.");
    }
}
