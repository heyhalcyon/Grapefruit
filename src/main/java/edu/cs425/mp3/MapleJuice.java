package edu.cs425.mp3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingDeque;

import static edu.cs425.mp3.Utility.*;

public class MapleJuice {

    private static final HashMap<Integer, Integer> senderPortMap = new HashMap<>(); // map node number to its sender port number

    private static final HashMap<Integer, Integer> receiverPortMap = new HashMap<>(); // map node number to its receiver port number

    private final int MASTER_NODE_NUMBER = 1; // Node number of master node

    private final long JOB_EXECUTION_PERIOD = 100; // Period to execute job

    private final long MAPLE_PROCESS_PERIOD = 1000; // master node check complete maple tasks periodically

    private final long JUICE_PROCESS_PERIOD = 1000; // master node check complete juice tasks periodically

    private final int SENDER_PORT_START = 8881; // Starting sender port number

    private final int RECEIVER_PORT_START = 9991; // Starting receiver port number

    public static int RESCHEDULE_PORT_DIFF = 20; // To calculate reschedule thread port

    private final int NODE_LIMIT = 30; // Maximum number of nodes allowed

    public static final Logger logger = LogManager.getLogger(MapleJuice.class); // logger instance used for logging (provided by log4j2 library)

    private final int LINE_PROCESS = 50; // Fixed number of lines for executable file to process at a time

    private final int nodeNumber; // Current node number

    private final boolean isMaster; // Declare whether the current node is a master or not

    private final int senderPort; // Current node's sender port number

    private final int receiverPort; // Current node's receiver port number

    public static int reschedulePort; // Reschedule port number

    private ArrayList<Task> mapleTaskList; // List of maple tasks

    private ArrayList<Task> juiceTaskList; // List of juice tasks

    private HashMap<String, String> keyFileMap; // intermediate key, file mapping

    private DatagramSocket senderUDPSocket; // UDP socket used to send message

    private DatagramSocket receiverUDPSocket; // Another UDP socket used to receive message

    private DatagramSocket rescheduleSocket; // Reschedule socket

    private boolean runReceiver = true; // Control receiver thread

    private boolean runJobThread = true; // Control job execution thread

    private boolean runReschedule = true; // Control reschedule thread

    private volatile Queue<Job> jobQueue; // Queue of jobs (maple job & juice jobs)

    private LinkedBlockingDeque<Integer> completeTaskQueue; // Queue of complete tasks

    private MembershipServer membershipServer; // Membership server

    private SdfsServer sdfsServer; // SDFS server

    private CopyOnWriteArraySet<Task> unscheduledTaskSet; // Set of unscheduled tasks

    private HashMap<Integer, Task> nodeTaskTable; // <node, Task> check what tasks are assigned to a given node

    private Thread handleUIThread = new HandleUIThread(); // Thread used to handle user input in CLI

    private Thread executeJobThread = new ExecuteJobThread(); // Thread used to execute job (FIFO)

    private Thread receiverThread = new ReceiverThead(); // Thread used to receive message

    private Thread rescheduleThread = new RescheduleThead(); // Thread used to reschedule incomeplete task upon node failure

    // Constructor, for initialization
    public MapleJuice(int nodeNumber, int mode) {
        initiatePortMap(senderPortMap, NODE_LIMIT, SENDER_PORT_START);
        initiatePortMap(receiverPortMap, NODE_LIMIT, RECEIVER_PORT_START);

        this.nodeNumber = nodeNumber;
        this.isMaster = nodeNumber == 1;
        this.senderPort = senderPortMap.get(nodeNumber);
        this.receiverPort = receiverPortMap.get(nodeNumber);
        if (isMaster) {
            reschedulePort = receiverPort + RESCHEDULE_PORT_DIFF;
        }

        this.jobQueue = new LinkedList<>();
        this.completeTaskQueue = new LinkedBlockingDeque<>();
        this.sdfsServer = new SdfsServer(nodeNumber, mode);
        this.membershipServer = this.sdfsServer.getMembershipServer();

        // Initiate sender socket
        try {
            this.senderUDPSocket = new DatagramSocket(senderPort);
        } catch (SocketException e) {
            logger.error("SdfsServer(): fail to create sender UDP socket");
            e.printStackTrace();
        }
    }

    // Start MapleJuice Server
    public void start() {
        // Start SDFS service
        sdfsServer.start();

        // Start all threads;
        handleUIThread.start();
        executeJobThread.start();
        receiverThread.start();
        if (isMaster) {
            rescheduleThread.start();
        }
    }

    // Stop MapleJuice Server
    public void stop() {
        sdfsServer.stop(); // Stop SDFS server first. Inside it, it will stop membership server.
        runJobThread = false;
        runReceiver = false;
        runReschedule = false;

        try {
            senderUDPSocket.close();

            if (receiverUDPSocket != null) {
                receiverUDPSocket.close();
            }

            if (isMaster && rescheduleSocket != null) {
                rescheduleSocket.close();
            }

            // Waiting for the completion of all threads
            rescheduleThread.join();
            executeJobThread.join();
            receiverThread.join();
        } catch (Exception e) {
            logger.info("MapleJuice::stop(): Ignore exceptions that occur during server stopping. Stop the server anyway.");
        } finally {
            System.out.println("MapleJuice server stopped");
            System.exit(0);
        }
    }

    // This method will only be called on the master node. Master node calls this function to schedule Maple tasks.
    public void mapleMaster(String executableFile, int taskNum, String sifPrefix, int initializerId, String sdfsSrcDir) {
        long start = System.currentTimeMillis();

        logger.info("Master starts scheduling Maple tasks for new Maple job (exe file: " + executableFile + ", task number: " + taskNum + ", sifPrefix: " + sifPrefix + ", sdfsSrcDir: " + sdfsSrcDir + ")");
        ArrayList<String> inputFileList = sdfsServer.getFileListFromSdfsDir(sdfsSrcDir);

        int totalFileNum = inputFileList.size();
        int taskPayload = totalFileNum / taskNum; // To calculate how many files a task should include.
        int remainder = totalFileNum % taskNum;
        int counter = 0;

        // Clear the data structure below
        mapleTaskList = new ArrayList<>();
        ArrayList<Task> scheduledTaskList = new ArrayList<>();
        unscheduledTaskSet = new CopyOnWriteArraySet<>();
        nodeTaskTable = new HashMap<>();
        completeTaskQueue.clear();

        HashSet<Integer> availableNodes = sdfsServer.getMembershipServer().getNormalNodeNumbers(true);
        availableNodes.remove(MASTER_NODE_NUMBER); // do not assign tasks to the master node: 1. Master node does not run Maple tasks. It only assigns tasks to worker nodes.

        // assign input files to tasks
        for (int i = 0; i < taskNum && counter < totalFileNum; i++) {
            ArrayList<String> taskInputFileList = new ArrayList<>();
            int curPayload = (i < remainder) ? taskPayload + 1 : taskPayload; // assign one additional file to the first 'remainder' tasks
            for (int j = 0; j < curPayload; j++) {
                taskInputFileList.add(inputFileList.get(counter++));
            }

            Task task;
            if (availableNodes.size() > 0) {
                int workerNodeId = electWorker(taskInputFileList, availableNodes);
                availableNodes.remove(workerNodeId);
                task = new Task(i, workerNodeId, TaskType.MAPLE, TaskStatus.SCHEDULED, executableFile, taskInputFileList);
                nodeTaskTable.put(workerNodeId, task);
                scheduledTaskList.add(task);
            } else { // No more available node. All normal nodes are assigned a task (busy).
                task = new Task(i, -1, TaskType.MAPLE, TaskStatus.UNSCHEDULED, executableFile, taskInputFileList);
                unscheduledTaskSet.add(task);
            }
            mapleTaskList.add(task);
        }

//        printTaskList(mapleTaskList);
        HashMap<Integer, String> ipMap = sdfsServer.getIpMap();

        for (Task scheduledTask : scheduledTaskList) {
            logger.info("Master sending task (task ID: " + scheduledTask.getTaskID() + ") to node (node: ID: " + scheduledTask.getNodeID() + ")");
            sendTaskToNode(scheduledTask, ipMap, TaskType.MAPLE);
        }

        int processedTaskNum = 0;

        keyFileMap = new HashMap<>();

        while (processedTaskNum < mapleTaskList.size()) {

            try {
                Thread.sleep(MAPLE_PROCESS_PERIOD);
            } catch (InterruptedException e) {
                logger.error("mapleMaster(): InterruptedException occurs.");
                e.printStackTrace();
            }


            while (!completeTaskQueue.isEmpty()) { // Processing complete maple tasks
                logger.debug("mapleMaster(): Processing list of completed maple tasks...");
                int taskId = completeTaskQueue.remove(); // dequeue
                Task completeTask = mapleTaskList.get(taskId); // new complete task

                // Update available nodes
                int newAvailableNodeId = completeTask.getNodeID();
                availableNodes.add(newAvailableNodeId);

                logger.info("mapleMaster(): Master start processing complete task (task ID: " + taskId);
                String taskOutputFileName = completeTask.getOutputFileName();

                logger.debug("mapleMaster(): Downloading worker's output file: " + taskOutputFileName);
                HashMap<String, ArrayList<String>> kVs = fileRequest(ipMap.get(newAvailableNodeId), sdfsServer.receiverPortMap.get(newAvailableNodeId) + sdfsServer.TCP_PORT_DIFFERENCE, taskOutputFileName, "master_tmp_" + taskOutputFileName, true);
                if (kVs == null) {
                    unscheduledTaskSet.add(completeTask);
                    logger.info("mapleMaster(): Cannot process the task (task ID: " + taskId + ") due to file request exception. Re-add the task to unscheduled task set. Will be schedule later.");
                    continue;
                }
                logger.debug("mapleMaster(): Downloading the file succeeds!");
                completeTask.setTaskStatus(TaskStatus.PROCESSED);

                try {
                    BufferedWriter bufferedWriter = null;
                    FileWriter fileWriter = null;

                    for (String key : kVs.keySet()) {
                        String filename = "local_" + sifPrefix + "_" + key;
                        if (!keyFileMap.containsKey(key)) {
                            File file = new File(filename);
                            file.createNewFile();
                            keyFileMap.put(key, filename);
                        }

                        fileWriter = new FileWriter(filename, true);
                        bufferedWriter = new BufferedWriter(fileWriter);
                        for (String val : kVs.get(key)) {
                            bufferedWriter.write(val + "\n");
                        }
                        bufferedWriter.flush();
                    }

                    if (bufferedWriter != null) {
                        bufferedWriter.close();
                    }
                } catch (IOException e) {
                    logger.error("mapleMaster(): IOException occurs");
                    e.printStackTrace();
                }

                ++processedTaskNum;
                logger.info("Finish processing complete maple task (task ID: " + taskId + ")");
            }

            if (!unscheduledTaskSet.isEmpty() && !availableNodes.isEmpty()) {
                logger.debug("Processing unscheduled maple task set...");
                HashSet<Task> newScheduledTaskSet = new HashSet<>();
                for (Task unscheduledTask : unscheduledTaskSet) {
                    int workerId = electWorker(unscheduledTask.getInputFileList(), availableNodes);
                    if (workerId != -1) { // There exists an available node
                        unscheduledTask.setNodeID(workerId);
                        unscheduledTask.setTaskStatus(TaskStatus.SCHEDULED);

                        nodeTaskTable.put(workerId, unscheduledTask);
                        newScheduledTaskSet.add(unscheduledTask);

                        availableNodes.remove(workerId);

                        logger.info("mapleMaster(): Get new available node => master sending previously unscheduled maple task (task ID: " + unscheduledTask.getTaskID() + ") to node (node: ID: " + unscheduledTask.getNodeID() + ")");
                        sendTaskToNode(unscheduledTask, ipMap, TaskType.MAPLE);
                    }
                }
                unscheduledTaskSet.removeAll(newScheduledTaskSet);
            }
        }

        logger.info("mapleMaster(): All maple tasks are completed and processed.");
        logger.debug("Number of keys: " + keyFileMap.size());

        int count = 0;
        for (String localIntermediateFileName : keyFileMap.values()) {
            count++;
            logger.debug("juiceMaster(): Uploading intermediate file: " + localIntermediateFileName);
            sdfsServer.putHelper(localIntermediateFileName, localIntermediateFileName.substring("local_".length()));
            logger.debug("juiceMaster(): finish Uploading intermediate file: " + localIntermediateFileName);
            logger.debug("mapleMaster(): upload file count: " + count);
        }

        if (initializerId == MASTER_NODE_NUMBER) {
            System.out.println("Maple job completed");
        } else {
            logger.info("Sending maple job complete message to node (ID: " + initializerId + ")...");
            sendMsg(senderUDPSocket, ipMap.get(initializerId), receiverPortMap.get(initializerId), "[MAPLE_JOB_DONE]");
        }

        long end = System.currentTimeMillis();
        long duration = end - start;
        System.out.println("--------Takes " + duration + " ms to finish maple job------------");
    }

    // Elect an available worker node (with highest locality) for a given task. Return -1 if there exists no available node.
    private int electWorker(ArrayList<String> taskInputFileList, HashSet<Integer> availableNodes) {
        int electNode = -1;
        int vote = -1;

        for (int nodeID : availableNodes) {
            if (membershipServer.isNodeNormal(nodeID)) {
                int curVote = 0;
                for (String file : taskInputFileList) {
                    if (sdfsServer.fullFileList.get(file).getReplicaSet().contains(nodeID)) {
                        ++curVote;
                    }
                }
                if (curVote > vote) {
                    vote = curVote;
                    electNode = nodeID;
                }
                if (vote == taskInputFileList.size()) {
                    break;
                }
            }
        }
        return electNode;
    }

    // Master node sends tasks to worker nodes (via UDP)
    private void sendTaskToNode(Task task, HashMap<Integer, String> ipMap, TaskType taskType) {
        String taskStr = task.serialize();
        int assignedNode = task.getNodeID();
        String msgPrefix = taskType == TaskType.MAPLE ? "[ASSIGN_MAPLE]" : "[ASSIGN_JUICE]";
        sendMsg(senderUDPSocket, ipMap.get(assignedNode), receiverPortMap.get(assignedNode), msgPrefix + taskStr);
    }

    // For debugging. Print list of tasks.
    private void printTaskList(ArrayList<Task> taskList) {
        System.out.println(String.format("%1$-15s\t%2$-15s\t%3$-20s\t%4$-20s\t%5$-25s\t%6$-35s", "taskID", "nodeId", "taskType", "taskStatus", "exeFileName", "inputFileList"));
        for (Task task : taskList) {
            System.out.println(String.format("%1$-15s\t%2$-15s\t%3$-20s\t%4$-20s\t%5$-25s\t%6$-35s", task.getTaskID(), task.getNodeID(), task.getTaskType(), task.getTaskStatus(), task.getExeFileName(), task.getInputFileList().toString()));
        }
        System.out.println("----------------------Task List End--------------------------");
    }

    // Called on worker nodes. Worker nodes call this function to process tasks assigned by the master node
    private void mapleWorker(int taskID, String exeFileName, HashSet<String> inputFileList, String sourceIp) {
        // Download input files to local
        logger.debug("mapleWorker(): Downloading input files"); // Download task input files first
        downloadFilesToLocal(inputFileList);
        logger.debug("mapleWorker(): Download Done");

        // Remove executable file from input file list
        inputFileList.remove(exeFileName);

        // Create an output file
        String outputFileName = "local_maple_" + taskID + "_" + System.currentTimeMillis() + "_op"; // Create a local file to store task output
        File outputFile = new File(outputFileName);
        PrintWriter fileWriter;
        try {
            fileWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outputFile)), true);
        } catch (IOException e) {
            logger.error("mapleWorker(): IO Exception in creating printwriter of output file");
            e.printStackTrace();
            return;
        }

        // Read the input files, execute exe file for every LINE_PROCESS lines
        String localExePath = getLocalPath(exeFileName);
        String exeInputStr = "";
        int lineCount = 0;
        BufferedReader in = null;
        try {
            for (String inputFile : inputFileList) {
                String inputLocalPath = getLocalPath(inputFile);
                logger.debug("mapleWorker(): Processing input file: " + inputFile + " at path: " + inputLocalPath);
                File file = new File(inputLocalPath);
                in = new BufferedReader(new FileReader(file));

                String line;
                while ((line = in.readLine()) != null) {
                    exeInputStr += line + "\n";
                    lineCount++;
                    if (lineCount % LINE_PROCESS == 0) { // Process fixed number of lines at a time.
                        // execute the executable file for the current load
                        String[] exeCommand = {"python3", localExePath, exeInputStr}; // Support Python3 . Can be configured later.
//                        String[] exeCommand = {"java", "-jar", exeFileName, exeInputStr}; // Can also support Java. Can be configured.
                        runCommand(exeCommand, fileWriter);
                        exeInputStr = "";
                    }
                }
                logger.debug("mapleWorker(): Finish processing input file: " + inputFile);
            }

            if (exeInputStr.length() > 0) {
//                String[] exeCommand = {"java", "-jar", exeFileName, exeInputStr};
                String[] exeCommand = {"python3", localExePath, exeInputStr}; // Support Python3 . Can be configured later.
                runCommand(exeCommand, fileWriter);
            }

            if (in != null) {
                in.close();
            }
            fileWriter.close();

            // Worker completes a maple task => send "complete" message to master.
            logger.info("Worker (nodeId: " + this.nodeNumber + ")" + " completes a maple task (taskID: " + taskID + ") at time: " + System.currentTimeMillis());
            String message = "[MAPLE_TASK_DONE]" + taskID + "@" + outputFileName;
            sendMsg(senderUDPSocket, sourceIp, receiverPortMap.get(MASTER_NODE_NUMBER), message); // Send "task completes" message to the master.
        } catch (IOException e) {
            logger.error("mapleWorker(): IO Exception");
            e.printStackTrace();
            return;
        }
    }

    // Called only on the master node. Schedule juice tasks. Process worker output and generate juice final result.
    public void juiceMaster(String executableFile, int taskNum, String sifPrefix, int initializerId, String sdfsDestFileName, boolean deleteInput, ShuffleOption shuffleOption) {
        long start = System.currentTimeMillis();

        logger.info("Master starts scheduling Juice tasks for new Juice job (exe file: " + executableFile + ", task number: " +
            taskNum + ", sifPrefix: " + sifPrefix + ", sdfsDestFileName: " + sdfsDestFileName + ")");

        // find the SDFS intermediate files using sifPrefix
        ArrayList<String> intermediateFiles = sdfsServer.getIntermediateFiles(sifPrefix);

        ArrayList<ArrayList<String>> filePartition = shuffle(intermediateFiles, taskNum, shuffleOption);

        // Clear storage
        juiceTaskList = new ArrayList<>();
        ArrayList<Task> scheduledTaskList = new ArrayList<>();
        unscheduledTaskSet = new CopyOnWriteArraySet<>();
        nodeTaskTable = new HashMap<>();
        completeTaskQueue.clear();

        HashSet<Integer> availableNodes = sdfsServer.getMembershipServer().getNormalNodeNumbers(true);
        availableNodes.remove(MASTER_NODE_NUMBER); // do not assign tasks to the master node: 1. Master node does not perform task itself. Instead, it assign tasks to other nodes.

        int curTaskId = 0;

        // assign input files to tasks
        for (ArrayList<String> taskInputFileList : filePartition) {
            if (taskInputFileList.size() > 0) {
                Task task;
                if (availableNodes.size() > 0) {
                    int workerNodeId = electWorker(taskInputFileList, availableNodes);
                    availableNodes.remove(workerNodeId);
                    task = new Task(curTaskId++, workerNodeId, TaskType.JUICE, TaskStatus.SCHEDULED, executableFile, taskInputFileList);
                    nodeTaskTable.put(workerNodeId, task);
                    scheduledTaskList.add(task);
                } else { // No more available node. All normal nodes are assigned a task (busy).
                    task = new Task(curTaskId++, -1, TaskType.JUICE, TaskStatus.UNSCHEDULED, executableFile, taskInputFileList);
                    unscheduledTaskSet.add(task);
                }
                juiceTaskList.add(task);
            }
        }

        logger.debug("juiceMaster(): juice task list: ");
//        printTaskList(juiceTaskList);

        HashMap<Integer, String> ipMap = sdfsServer.getIpMap();
        for (Task scheduledTask : scheduledTaskList) { // Send scheduled tasks to worker nodes.
            logger.info("Master sending task (task ID: " + scheduledTask.getTaskID() + ") to node (node: ID: " + scheduledTask.getNodeID() + ")");
            sendTaskToNode(scheduledTask, ipMap, TaskType.JUICE);
        }

        int processedTaskNum = 0; // Record the number of tasks that has been processed.

        String localDestFileName = "local_" + sdfsDestFileName;
        BufferedWriter bufferedWriter = null;
        FileWriter fileWriter = null;

        try {
            File file = new File(localDestFileName); // Create juice result file
            file.createNewFile();

            fileWriter = new FileWriter(localDestFileName, true);
            bufferedWriter = new BufferedWriter(fileWriter);
        } catch (IOException e) {
            logger.error("juiceMaster(): IO Exception in creating the sdfsDestFile");
            e.printStackTrace();
        }

        while (processedTaskNum < juiceTaskList.size()) {
            try {
                Thread.sleep(JUICE_PROCESS_PERIOD);
            } catch (InterruptedException e) {
                logger.error("juiceMaster(): InterruptedException occurs.");
                e.printStackTrace();
            }

            logger.info("juiceMaster(): Processing list of completed juice tasks...");
            while (!completeTaskQueue.isEmpty()) {
                int taskId = completeTaskQueue.remove(); // dequeue
                Task completeTask = juiceTaskList.get(taskId); // new complete task

                // Update available nodes
                int newAvailableNodeId = completeTask.getNodeID();
                availableNodes.add(newAvailableNodeId);

                logger.info("juiceMaster(): Master starts processing completed task (task ID: " + taskId + ")");
                String taskOutputFileName = completeTask.getOutputFileName();

                logger.debug("juiceMaster(): Downloading worker's output file: " + taskOutputFileName);
                HashMap<String, ArrayList<String>> kVs = fileRequest(ipMap.get(newAvailableNodeId),
                    sdfsServer.receiverPortMap.get(newAvailableNodeId) + sdfsServer.TCP_PORT_DIFFERENCE,
                    taskOutputFileName, "master_tmp_" + taskOutputFileName, true);
                if (kVs == null) {
                    unscheduledTaskSet.add(completeTask);
                    logger.info("juiceMaster(): Cannot process the task (task ID: " + taskId + ") due to file request exception. Re-add the task to unscheduled task set. Will be schedule later.");
                    continue;
                }
                logger.debug("juiceMaster(): Download succeeds!");
                completeTask.setTaskStatus(TaskStatus.PROCESSED);

                try { // Write to result file
                    for (String key : kVs.keySet()) {
                        for (String val : kVs.get(key)) {
                            bufferedWriter.write(key + "," + val + "\n");
                        }
                    }

                } catch (IOException e) {
                    logger.error("juiceMaster(): IOException occurs");
                    e.printStackTrace();
                }

                ++processedTaskNum;
                logger.info("Finish processing complete juice task (task ID: " + taskId + ")");
            }

            logger.debug("juiceMaster(): Processing unscheduled juice task set...");
            if (!unscheduledTaskSet.isEmpty()) { // Schedule unscheduled tasks to available nodes
                HashSet<Task> newScheduledTaskSet = new HashSet<>();
                for (Task unscheduledTask : unscheduledTaskSet) {
                    int workerId = electWorker(unscheduledTask.getInputFileList(), availableNodes);
                    if (workerId != -1) { // There exists an available node
                        unscheduledTask.setNodeID(workerId);
                        unscheduledTask.setTaskStatus(TaskStatus.SCHEDULED);

                        nodeTaskTable.put(workerId, unscheduledTask); // update <node, task> mapping
                        newScheduledTaskSet.add(unscheduledTask);

                        availableNodes.remove(workerId);

                        logger.info("juiceMaster(): Get new available node => master sending previously unscheduled juice task (task ID: " + unscheduledTask.getTaskID() + ") to node (node: ID: " + unscheduledTask.getNodeID() + ")");
                        sendTaskToNode(unscheduledTask, ipMap, TaskType.JUICE);
                    }
                }
                unscheduledTaskSet.removeAll(newScheduledTaskSet);
            }
//            logger.debug("juiceMaster(): juice task list:...");
//            printTaskList(juiceTaskList);
        }

        try {
            if (bufferedWriter != null) {
                bufferedWriter.close();
            }
        } catch (IOException e) {
            logger.error("juiceMaster(): IOException occurs");
            e.printStackTrace();
        }

        logger.info("juiceMaster(): All juice tasks are completed and processed.");
        // Upload the juice output to SDFS
        sdfsServer.putHelper(localDestFileName, sdfsDestFileName);

        if (initializerId == MASTER_NODE_NUMBER) {
            System.out.println("Juice job completed");
        } else {
            logger.info("Sending juice success message to juice job initializer (node ID: " + initializerId + ")...");
            sendMsg(senderUDPSocket, ipMap.get(initializerId), receiverPortMap.get(initializerId), "[JUICE_JOB_DONE]");
        }

        // Delete the intermediate files
        if (deleteInput) {
            for (String intermediateFile : intermediateFiles) {
                sdfsServer.deleteHelper(intermediateFile);
            }
            logger.info("Intermediate Files has been deleted from SDFS.");
        }

        long end = System.currentTimeMillis();
        long duration = end - start;
        System.out.println("--------Takes " + duration + " ms to finish juice job------------");
    }

    // Worker node process a juice task given the executable file and the intermediate file list.
    private void juiceWorker(int taskID, String exeFileName, HashSet<String> intermediateFileList, String sourceIp) {
        // Download intermediate files and the executable file to local
        logger.debug("juiceWorker(): Downloading input files");
        downloadFilesToLocal(intermediateFileList);
        logger.debug("juiceWorker(): Download Done");

        // Remove executable file from input file list
        intermediateFileList.remove(exeFileName);

        // Create an output file
        String outputFileName = "local_juice_" + taskID + "_" + System.currentTimeMillis() + "_op";
        File outputFile = new File(outputFileName);
        PrintWriter fileWriter;
        try {
            fileWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outputFile)), true);
        } catch (IOException e) {
            logger.error("juiceWorker(): IO Exception in creating print writer of output file");
            e.printStackTrace();
            return;
        }
        String localExePath = getLocalPath(exeFileName);    // get the local path of the executable file

        // Execute exe file on each intermediate file
        try {
            BufferedReader in = null;
            for (String intermediateFile : intermediateFileList) {
                logger.debug("Processing intermediate file: " + intermediateFile);
                String inputLocalPath = getLocalPath(intermediateFile);
                File file = new File(inputLocalPath);
                in = new BufferedReader(new FileReader(file));
                String[] strArr = intermediateFile.split("_");
                String key = strArr[strArr.length - 1];  //format of intermediate filename: sifPrefix + "_" + key; key is the last element of the str array split by "_"
//                String[] exeCommand = {"java", "-jar", exeFileName, key, inputLocalPath}; // Support Java.
                String[] exeCommand = {"python3", localExePath, key, inputLocalPath}; // Support Python3.
                runCommand(exeCommand, fileWriter); // run the executable file
            }

            in.close();
            fileWriter.close();

            // Worker completes a juice task => send "complete" message to master.
            logger.info("Worker (nodeId: " + this.nodeNumber + ")" + " completes a juice task (taskID: " + taskID + ") at time: " + System.currentTimeMillis());
            String message = "[JUICE_TASK_DONE]" + taskID + "@" + outputFileName;
            sendMsg(senderUDPSocket, sourceIp, receiverPortMap.get(MASTER_NODE_NUMBER), message);

        } catch (IOException e) {
            logger.error("juiceWorker(): IO Exception");
            e.printStackTrace();
        }

    }

    // partition the intermediate files given the shuffle option and number of tasks
    private ArrayList<ArrayList<String>> shuffle(ArrayList<String> intermediateFiles, int taskNum, ShuffleOption shuffleOption) {
        ArrayList<ArrayList<String>> taskList = new ArrayList<>();
        int totalFileNum = intermediateFiles.size();

        if (shuffleOption == ShuffleOption.HASH) {
            for (int i = 0; i < taskNum; i++) {
                taskList.add(new ArrayList<>());
            }
            for (String file : intermediateFiles) {
                int bucket = Math.abs(file.hashCode()) % taskNum; // hash each file to a bucket ranging from 0 to taskNum-1
                taskList.get(bucket).add(file);
            }
        } else { // ShuffleOption == RANGE
            Collections.sort(intermediateFiles); // sort file names by key (file name = prefix + key)
            int taskPayload = totalFileNum / taskNum;  // evenly distribute the input files
            int remainder = totalFileNum % taskNum;
            int counter = 0;
            for (int i = 0; i < taskNum && counter < totalFileNum; i++) {
                ArrayList<String> taskInputFileList = new ArrayList<>();
                int curPayload = (i < remainder) ? taskPayload + 1 : taskPayload;  // assign one more file to the first <remainder> tasks
                for (int j = 0; j < curPayload; j++) {
                    taskInputFileList.add(intermediateFiles.get(counter++));
                }
                taskList.add(taskInputFileList);
            }
        }
        return taskList;
    }

    // get the local path of an SDFS file given SDFS file name
    private String getLocalPath(String sdfsFileName) {
        if (sdfsServer.getLocalFileList().contains(sdfsFileName)) {
            return sdfsServer.fullFileList.get(sdfsFileName).getLocalFilePath();
        } else {
            return "local" + "_" + this.nodeNumber + "_" + sdfsFileName;
        }
    }

    // Download input files that are in SDFS to local
    private void downloadFilesToLocal(HashSet<String> inputFileList) {
        sdfsServer.printLocalFileList();
        sdfsServer.printFullFileList();
        CopyOnWriteArraySet<String> localFileList = sdfsServer.getLocalFileList();
        for (String fileName : inputFileList) {
            if (!localFileList.contains(fileName)) {  // naming rule: local_[<node number>]_<SDFS file name>
                sdfsServer.getHelper(fileName, "local" + "_" + this.nodeNumber + "_" + fileName);
            }
        }
    }

    // Thread used to reschedule tasks
    private class RescheduleThead extends Thread {
        @Override
        public void run() {
            try {
                rescheduleSocket = new DatagramSocket(reschedulePort);
                while (runReschedule) {

                    byte[] buffer = new byte[2048];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                    rescheduleSocket.receive(packet);

                    InetAddress address = packet.getAddress();
                    String sourceIp = address.getHostAddress();
                    int port = packet.getPort();
                    packet = new DatagramPacket(buffer, buffer.length, address, port);

                    // MapleJuice get notified about a newly failed node from the MembershipServer
                    int failedNodeId = Integer.parseInt(new String(packet.getData(), 0, packet.getLength()).trim());
                    logger.info("RescheduleThread():: failed node: " + failedNodeId);

                    if (nodeTaskTable.containsKey(failedNodeId)) {
                        Task failedTask = nodeTaskTable.get(failedNodeId);
                        logger.debug("RescheduleThread(): failed task status: " + failedTask.getTaskID() + " " + failedTask.getTaskStatus());
                        if (failedTask.getTaskStatus() != TaskStatus.PROCESSED) { // The task (assigned to failed worker node)'s output has not been processed by master node.
                            failedTask.setTaskStatus(TaskStatus.UNSCHEDULED);
                            unscheduledTaskSet.add(failedTask);  // add the failed node's task to unscheduled task set.
                            logger.info("RescheduleThread(): due to node (node ID: " + failedNodeId + ")'s failure, master re-adds the task (task ID: " + failedTask.getTaskID() + ") to unscheduled task set. Waiting to be rescheduled.");
                        }
                    } else { // do nothing if the failed node has not been assigned a task
                        logger.debug("RescheduleThread(): Failed node (node ID: " + failedNodeId + ") has not been assigned a task.");
                    }
                }
            } catch (SocketException e) {
                logger.info("Closing Reschedule Socket");
            } catch (IOException e) {
                logger.error("RescheduleThread::run(): exception thrown");
                e.printStackTrace();
            } finally { // finally close the socket
                if (rescheduleSocket != null) {
                    rescheduleSocket.close();
                }
                logger.info("Reschedule Socket closed");
            }
        }
    }


    // Thread used to receive messages
    private class ReceiverThead extends Thread {
        @Override
        public void run() {
            try {
                receiverUDPSocket = new DatagramSocket(receiverPort);

                while (runReceiver) {
                    byte[] buffer = new byte[2048];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                    receiverUDPSocket.receive(packet);
                    if (membershipServer.node.getStatus() != 1) { // if the node is not in the group, do nothing
                        continue;
                    }

                    InetAddress address = packet.getAddress();
                    String sourceIp = address.getHostAddress();
                    int port = packet.getPort();
                    packet = new DatagramPacket(buffer, buffer.length, address, port);

                    String message = new String(packet.getData(), 0, packet.getLength()).trim();
                    logger.debug("ReceiverThread()::run(): receive message: " + message);

                    if (message.startsWith("[ASSIGN_MAPLE]")) {  // worker node receives a maple task from the master node, and start processing the task
                        String[] msg = message.substring("[ASSIGN_MAPLE]".length()).split("@");

                        int taskID = Integer.parseInt(msg[0]);
                        String exeFileName = msg[1];

                        HashSet<String> inputFileList = new HashSet<>();
                        Collections.addAll(inputFileList, msg[2].split(":"));
                        inputFileList.add(exeFileName);

                        logger.info("ReceiverThread()::run(): Worker get new Maple task (task ID:" + taskID + ") from master node at time: " + System.currentTimeMillis());
                        mapleWorker(taskID, exeFileName, inputFileList, sourceIp);

                    } else if (message.startsWith("[MAPLE_TASK_DONE]")) { // master node get notified about a newly finished maple task from worker nodes
                        String[] msgArr = message.substring("[MAPLE_TASK_DONE]".length()).split("@");
                        int taskID = Integer.parseInt(msgArr[0]);
                        String outputFileName = msgArr[1];

                        mapleTaskList.get(taskID).setOutputFileName(outputFileName);
                        mapleTaskList.get(taskID).setTaskStatus(TaskStatus.COMPLETE);
                        completeTaskQueue.add(taskID);  // update the completed task queue

                        logger.info("ReceiverThread()::run(): Master receives a complete maple task (Task ID: " + taskID + ") from worker. Add task to complete maple task queue.");
                        logger.debug("ReceiverThread()::run(): Complete maple task queue:");

                    } else if (message.startsWith("[MAPLE_REQUEST]")) {     // master receives a maple job from a worker node
                        logger.info("ReceiverThread()::run(): Receive a maple job request from a worker node");
                        String jobStr = message.substring("[MAPLE_REQUEST]".length());
                        MapleJob mapleJob = MapleJob.deserializeJobStr(jobStr);
                        jobQueue.add(mapleJob);
                        logger.info("ReceiverThread()::run(): New maple job added to the master node's job queue");

                    } else if (message.startsWith("[MAPLE_JOB_DONE]")) {  // worker node get notified about a finished maple job
                        System.out.println("ReceiverThread()::run(): Maple job completed.");

                    } else if (message.startsWith("[JUICE_REQUEST]")) {  // master receives a juice job from a worker node
                        String jobStr = message.substring("[JUICE_REQUEST]".length());
                        JuiceJob juiceJob = JuiceJob.deserializeJobStr(jobStr);
                        jobQueue.add(juiceJob);
                        logger.info("ReceiverThread()::run(): New juice job added to the master node's job queue");

                    } else if (message.startsWith("[ASSIGN_JUICE]")) {  // worker node receives a juice task from the master node, and start processing the task
                        String[] msg = message.substring("[ASSIGN_JUICE]".length()).split("@");

                        int taskID = Integer.parseInt(msg[0]);
                        String exeFileName = msg[1];

                        HashSet<String> inputFileList = new HashSet<>();
                        Collections.addAll(inputFileList, msg[2].split(":"));
                        inputFileList.add(exeFileName);

                        logger.info("ReceiverThread()::run(): Worker get new Juice task (task ID:" + taskID + ") from master node at time: " + System.currentTimeMillis());
                        juiceWorker(taskID, exeFileName, inputFileList, sourceIp);

                    } else if (message.startsWith("[JUICE_TASK_DONE]")) {  // master node get notified about a newly finished juice task from a worker node
                        String[] msgArr = message.substring("[JUICE_TASK_DONE]".length()).split("@");
                        int taskID = Integer.parseInt(msgArr[0]);
                        String outputFileName = msgArr[1];

                        juiceTaskList.get(taskID).setOutputFileName(outputFileName);
                        juiceTaskList.get(taskID).setTaskStatus(TaskStatus.COMPLETE);
                        completeTaskQueue.add(taskID);  // update complete task queue

                        logger.info("ReceiverThread()::run(): Master receives a complete juice task (Task ID: " + taskID + ") from worker. Add task to complete juice task queue.");
                        logger.debug("ReceiverThread()::run(): Complete juice task queue:");

                    } else if (message.startsWith("[JUICE_JOB_DONE]")) {  // worker node get notified about a finished juice job
                        System.out.println("Juice job completed.");
                    }
                }
            } catch (SocketException e) {
                logger.info("ReceiverThread()::run(): Closing receiver socket");
            } catch (IOException e) {
                logger.error("ReceiverThread::run(): exception thrown");
                e.printStackTrace();
            } finally { // finally close the socket
                if (receiverUDPSocket != null) {
                    receiverUDPSocket.close();
                }
                logger.info("ReceiverThread()::run(): Receiver socket closed");
            }
        }
    }

    // Thread used to execute jobs in the job queue
    private class ExecuteJobThread extends Thread {
        @Override
        public void run() {
            while (runJobThread) {
                try {
                    Thread.sleep(JOB_EXECUTION_PERIOD); // Decrease CPU load
                } catch (InterruptedException e) {
                    logger.error("ExecuteJobThread:: run(): InterruptedException occurs.");
                    e.printStackTrace();
                }

                HashMap<Integer, String> ipMap = sdfsServer.getIpMap();

                // Process maple or juice jobs. If the current node is master, then execute the job. If the current node is worker, then send the job to the master.
                while (!jobQueue.isEmpty()) {
                    Job job = jobQueue.poll();

                    if (isMaster && (job instanceof MapleJob)) {
                        logger.info("ExecuteJobThread::run(): Master starts executing a new Maple Job...");
                        MapleJob mapleJob = (MapleJob) job;
                        mapleMaster(mapleJob.getExecutableFile(), mapleJob.getTaskNum(), mapleJob.getSifPrefix(), mapleJob.getInitializerId(), mapleJob.getSdfsSrcDir());
                    } else if (isMaster && (job instanceof JuiceJob)) {
                        logger.info("ExecuteJobThread::run(): Master starts executing a new Juice Job...");
                        JuiceJob juiceJob = (JuiceJob) job;
                        juiceMaster(juiceJob.getExecutableFile(), juiceJob.getTaskNum(), juiceJob.getSifPrefix(), juiceJob.getInitializerId(), juiceJob.getSdfsDestFilename(), juiceJob.getDeleteInput(), juiceJob.getShuffleOption());
                    } else if (job instanceof MapleJob) {
                        logger.info("ExecuteJobThread::run(): Worker sends a new Maple Job to the master node...");
                        MapleJob mapleJob = (MapleJob) job;
                        sendMsg(senderUDPSocket, ipMap.get(MASTER_NODE_NUMBER), receiverPortMap.get(MASTER_NODE_NUMBER), "[MAPLE_REQUEST]" + mapleJob.serializeJob());
                    } else {
                        logger.info("ExecuteJobThread::run(): Worker sends a new Juice Job to the master node...");
                        JuiceJob juiceJob = (JuiceJob) job;
                        sendMsg(senderUDPSocket, ipMap.get(MASTER_NODE_NUMBER), receiverPortMap.get(MASTER_NODE_NUMBER), "[JUICE_REQUEST]" + juiceJob.serializeJob());
                    }
                }
            }
        }
    }

    // Thread used to handle user input from console
    private class HandleUIThread extends Thread {
        @Override
        public void run() {
            System.out.println("Welcome to SDFS CLI...");
            System.out.println("Please enter the following commands to start: " + "\n\n" +
                "-------------------------------------------------------------------------------------------------\n" +
                "info: print node info" + "\n" +
                "memlist: print membership list" + "\n" +
                "join: request to join the group" + "\n" +
                "leave: request to leave the group" + "\n" +
                "switch: switch failure detection protocol" + "\n" +
                "maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>: start a maple job" + "\n" +
                "juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> <delete_input>={0,1} <shuffling>: start a juice job" + "\n" +
                "put <localfilename> <sdfsfilename>: insert/update a local file to SDFS" + "\n" +
                "get <sdfsfilename> <localfilename>: read an SDFS file to local path" + "\n" +
                "delete <sdfsfilename>: delete a file from SDFS" + "\n" +
                "ls <sdfsfilename>: list all VM addresses where the given SDFS file is being stored" + "\n" +
                "store: list all SDFS files being stored at the local machine" + "\n" +
                "stop: stop SDFS server" + "\n" +
                "-------------------------------------------------------------------------------------------------\n");

            Scanner scanner = new Scanner(System.in);
            String cmd;
            boolean stopHandler = true;

            while (stopHandler && (cmd = scanner.nextLine()) != null) { // Keep handling users' input until server stops.
                String[] params = cmd.trim().split("\\s+");
                cmd = params[0];
                switch (cmd) {
                    case "info": // print current node's information, including node id, heartbeat counter, unix timestamp when node was created, translated timestamp and node status.
                        membershipServer.printNodeInfo();
                        break;
                    case "memlist": // print the membership list of the group (including nodes that are normal, left, and failed).
                        membershipServer.printMembershipList();
                        break;
                    case "task":
                        printTaskList(mapleTaskList);
                        break;
                    case "join":
                        if (params.length == 2) { // Users can specify using which protocol to join/rejoin the group. If not, we will use the selected protocol when node starts.
                            membershipServer.protocol = Integer.parseInt(params[1]);
                            logger.info("Joining the group using protocol: " + (membershipServer.protocol == 1 ? "All-to-All" : "Gossip"));
                        }

                        if (!membershipServer.isIntroducer) {
                            if (membershipServer.node.getMembers().size() == 0) { // check if node is in the group. If so, tell users that node is in the group.
                                if (membershipServer.node.getStatus() == 2) {
                                    membershipServer.node = new Node(membershipServer.nodeNumber, membershipServer.ipAddr, System.currentTimeMillis());
                                }
                                int resp = membershipServer.joinHelper(); // Call jonHelper() to send join request to introducer
                                if (resp == 0) { // resp = 0 stands for success
                                    System.out.println("Join request succeeded");
                                } else {
                                    System.out.println("Join request failed. Try joining again.");
                                }
                            } else { // Node is already in the group. No need to join.
                                System.out.println("Already joined");
                            }
                        } else {
                            if (membershipServer.node.getStatus() == 1) { // Status 1: normal. It is unnecessary for a normal introducer to rejoin the group.
                                System.out.println("This node is an introducer (already in the group). Try joining other nodes.");
                            } else if (membershipServer.node.getStatus() == 2) { // Status 2: leave. Introducer left before. Now create a new to let introducer rejoins the group automatically.j
                                membershipServer.node = new Node(membershipServer.nodeNumber, membershipServer.ipAddr, System.currentTimeMillis());
                                logger.info("Introducer rejoins the group. Succeed!");
                            }
                        }
                        break;
                    case "leave": // Users wants the current node to leave the group
                        if (membershipServer.node.getMembers().size() == 0 && !membershipServer.isIntroducer) {
                            System.out.println("Node is not in the group. No group to leave.");
                        } else {
                            //empty file list
                            sdfsServer.fullFileList = new HashMap<>();
                            sdfsServer.localFileList = new CopyOnWriteArraySet<>();
                            membershipServer.leaveHelper(); // Call leaveHelper() to send leave request.
                        }
                        break;
                    case "switch": // handle users' switching protocol request
                        membershipServer.switchHelper(); // Call switch helper() to send switch request.
                        break;
                    case "maple":
                        if (params.length != 5) {
                            System.out.println("Please enter exactly 4 arguments indicating maple_exe (user-specified executable file), number of Maple tasks, SDFS intermediate maple output file name prefix, and SDFS input files' source directory.");
                            break;
                        }
                        String sdfsMapleExeName = params[1], sifPrefix = params[3], sdfsSrcDir = params[4];
                        int numMapleTask = Integer.parseInt(params[2]);
                        MapleJob mapleJob = new MapleJob(numMapleTask, sdfsMapleExeName, sifPrefix, nodeNumber, sdfsSrcDir);
                        jobQueue.add(mapleJob);
                        logger.info("New maple job added to the job queue");
                        break;
                    case "juice":
                        if (params.length != 7) {
                            System.out.println("Please enter exactly 6 arguments indicating juice_exe (user-specified executable file), number of Juice tasks, SDFS intermediate maple output file name prefix, Juice output file name, input file delete option, and shuffle grouping mechanism.");
                            break;
                        }
                        String sdfsJuiceExeName = params[1], sdfsDestFileName = params[4];
                        int numJuiceTask = Integer.parseInt(params[2]);
                        sifPrefix = params[3];
                        boolean deleteInput = Integer.parseInt(params[5]) == 1 ? true : false;
                        ShuffleOption shuffleOption = Integer.parseInt(params[6]) == 1 ? ShuffleOption.HASH : ShuffleOption.RANGE;

                        JuiceJob juiceJob = new JuiceJob(numJuiceTask, sdfsJuiceExeName, sifPrefix, nodeNumber, sdfsDestFileName, deleteInput, shuffleOption);
                        jobQueue.add(juiceJob);
                        logger.info("New juice job added to the master node's job queue");
                        break;
                    case "put": // upload SDFS file
                        if (params.length != 3) {
                            System.out.println("Please enter exactly 2 arguments indicating local file name and SDFS file name.");
                            break;
                        }
                        if (membershipServer.node.getStatus() != 1) {
                            System.out.println("Current node is not in the group. Try restarting the node or joining the group first.");
                            break;
                        }
                        String localFileName = params[1], sdfsFileName = params[2];
                        File localFile = new File(localFileName);
                        if (!localFile.exists()) {
                            System.out.println("Local file not found. Please enter a path of existing local file to upload to SDFS.");
                            break;
                        }
                        sdfsServer.putHelper(localFileName, sdfsFileName);
                        break;
                    case "get": // download SDFS file
                        if (params.length != 3) {
                            System.out.println("please enter exactly 2 arguments indicating sdfsfilename and localfilename");
                            break;
                        }
                        if (membershipServer.node.getStatus() != 1) {
                            System.out.println("Current node is not in the group. Try restarting the node or joining the group first.");
                            break;
                        }
                        sdfsServer.getHelper(params[1], params[2]);
                        break;
                    case "delete": // delete SDFS file entry
                        if (params.length != 2) {
                            System.out.println("please enter exactly 1 arguments indicating the sdfsfilename to be removed\");");
                        }
                        sdfsServer.deleteHelper(params[1]); // params[1] = SDFS file name
                        break;
                    case "ls": // show all nodes' ip address which stores the input SDFS file
                        if (params.length != 2) {
                            System.out.println("please enter exactly 1 arguments indicating the sdfsfilename to find\");");
                            break;
                        }
                        sdfsServer.globalFinder(params[1]); // params[1]: SDFS file name
                        break;
                    case "store": // show all SDFS files stored locally
                        sdfsServer.printLocalFileList();
                        break;
                    case "global": //print the full file list
                        sdfsServer.printFullFileList();
                        break;
                    case "stop": // handle users' stopping server request
                        System.out.println("Bye");
                        stopHandler = false;
                        MapleJuice.this.stop();
                        break;
                    case "put-wg-file": // put web graph test files and executable files
                        sdfsServer.putHelper("small-web-graphaa", "/maple-src/SD-wg-1");
                        sdfsServer.putHelper("small-web-graphab", "/maple-src/SD-wg-2");
                        sdfsServer.putHelper("small-web-graphac", "/maple-src/SD-wg-3");
                        sdfsServer.putHelper("small-web-graphad", "/maple-src/SD-wg-4");
                        sdfsServer.putHelper("wg_maple.py", "/SD-maple");
                        sdfsServer.putHelper("wg_juice.py", "/SD-juice");
                        logger.info("Web graph (wg) test files has been uploaded");
                        break;
                    case "wg-maple-test": // web graph maple test
                        Job testWgMapleJob = new MapleJob(4, "SD-maple", "SD-wg-inter", nodeNumber, "/maple-src/");
                        jobQueue.add(testWgMapleJob);
                        logger.info("New Test Maple job added to the job queue");
                        break;
                    case "wg-juice-test": // web graph juice test
                        if (params.length != 2) {
                            System.out.println("Please enter exactly 1 argument indicating SDFS Dest File Name.");
                            break;
                        }
                        String sdfsWgDestFile = params[1];
                        Job testWgJuiceJob = new JuiceJob(4, "SD-juice", "SD-wg-inter", nodeNumber, sdfsWgDestFile, false, ShuffleOption.HASH);
                        jobQueue.add(testWgJuiceJob);
                        break;
                    case "put-win-file":  // put condorcet winner test files and executable files
                        sdfsServer.putHelper("win-data-aa", "/maple1-src/SD-win-1");
                        sdfsServer.putHelper("win-data-ab", "/maple1-src/SD-win-2");
                        sdfsServer.putHelper("win-data-ac", "/maple1-src/SD-win-3");
                        sdfsServer.putHelper("win-data-ad", "/maple1-src/SD-win-4");
                        sdfsServer.putHelper("win_maple1.py", "/SD-maple1");
                        sdfsServer.putHelper("win_juice1.py", "/SD-juice1");
                        sdfsServer.putHelper("win_maple2.py", "/SD-maple2");
                        sdfsServer.putHelper("win_juice2.py", "/SD-juice2");
                        logger.info("Condorcet Win test files has been uploaded");
                        break;
                    case "put-small-win": // put condorcet winner small test files and executable files
                        sdfsServer.putHelper("win-1", "/maple1-src/SD-win-1");
                        sdfsServer.putHelper("win-2", "/maple1-src/SD-win-2");
                        sdfsServer.putHelper("win-3", "/maple1-src/SD-win-3");
                        sdfsServer.putHelper("win-4", "/maple1-src/SD-win-4");
                        sdfsServer.putHelper("win_maple1.py", "/SD-maple1");
                        sdfsServer.putHelper("win_juice1.py", "/SD-juice1");
                        sdfsServer.putHelper("win_maple2.py", "/SD-maple2");
                        sdfsServer.putHelper("win_juice2.py", "/SD-juice2");
                        break;
                    case "win-maple1-test": // condorcet winner maple1 test
                        Job testWinMapleJob1 = new MapleJob(4, "SD-maple1", "SD-win1-inter", nodeNumber, "/maple1-src/");
                        jobQueue.add(testWinMapleJob1);
                        logger.info("New Test Maple job added to the job queue");
                        break;
                    case "win-juice1-test":  // condorcet winner juice1 test
                        if (params.length != 2) {
                            System.out.println("Please enter exactly 1 argument indicating SDFS Dest File Name.");
                            break;
                        }
                        String sdfsWinJuice1DestFile = params[1];
                        Job testWinJuiceJob1 = new JuiceJob(4, "SD-juice1", "SD-win1-inter", nodeNumber, sdfsWinJuice1DestFile, false, ShuffleOption.HASH);
                        jobQueue.add(testWinJuiceJob1);
                        break;
                    case "win-maple2-test":   // condorcet winner maple2 test
                        Job testWinMapleJob2 = new MapleJob(1, "SD-maple2", "SD-win2-inter", nodeNumber, "/maple2-src/");
                        jobQueue.add(testWinMapleJob2);
                        logger.info("New Test Maple job added to the job queue");
                        break;
                    case "win-juice2-test":   // condorcet winner juice2 test
                        if (params.length != 2) {
                            System.out.println("Please enter exactly 1 argument indicating SDFS Dest File Name.");
                            break;
                        }
                        String sdfsWinJuice2DestFile = params[1];
                        Job testWinJuiceJob2 = new JuiceJob(1, "SD-juice2", "SD-win2-inter", nodeNumber, sdfsWinJuice2DestFile, false, ShuffleOption.HASH);
                        jobQueue.add(testWinJuiceJob2);
                        break;
                    default: // Users' input does not match any case above. Prompt users to enter a valid command.
                        System.out.println("Invalid command. Please enter a valid command.");
                }
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Invalid arguments number: please enter arguments as described below.");
            System.out.println("1st argument indicates the node number.");
            System.out.println("2nd argument indicates the deployment mode: 1 for local testing, 2 for production mode.");
            return;
        }
        int nodeNumber = Integer.parseInt(args[0]); // Each vm has a fixed node number. Node number is not changed even after vm fails and restarts.
        int mode = Integer.parseInt(args[1]);

        System.out.println("Starting MapleJuice server process...");
        MapleJuice mapleJuice = new MapleJuice(nodeNumber, mode);
        mapleJuice.start();
        System.out.println("MapleJuice started.");
    }
}
