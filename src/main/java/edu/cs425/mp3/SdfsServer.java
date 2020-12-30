package edu.cs425.mp3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

import static edu.cs425.mp3.Utility.*; // import utility function

/**
 * SdfsServer class: model server of simple distributed file server (SDFS)
 */
public class SdfsServer {

    private static final HashMap<Integer, Integer> senderPortMap = new HashMap<Integer, Integer>(); // map node number to its sender port number

    public static final HashMap<Integer, Integer> receiverPortMap = new HashMap<Integer, Integer>(); // map node number to its receiver port number

    private int SENDER_PORT_START = 6661; // Starting sender port number

    private int RECEIVER_PORT_START = 7771; // Starting receiver port number

    private int NODE_LIMIT = 10; // Maximum number of nodes allowed

    public final int REPLICATION_NUMBER = 3; // Replication factor (each file in SDFS should have 4 copies in total)

    private final int GOSSIP_PROTOCOL = 2; // Use gossip-style failure detection protocol in SDFS

    public static final int TCP_PORT_DIFFERENCE = 10; // Fixed port number difference between UDP port and TCP port

    private final int REREPLICATION_PORT_DIFFERENCE = 20; // fixed port number difference for re-replication socket port

    private int nodeNumber; // Node number of current node

    public HashMap<String, SdfsFile> fullFileList = new HashMap<>(); // formation: Key: SDFS file name, Value: associated SDFS file object

    public CopyOnWriteArraySet<String> localFileList = new CopyOnWriteArraySet<>(); // List of *SDFS* files stored locally

    public static final Logger logger = LogManager.getLogger(SdfsServer.class); // logger instance used for logging (provided by log4j2 library)

    private MembershipServer membershipServer; // membership server for failure detection, new node's joining, leaving, etc.

    private Thread receiverThread = new ReceiverThread(); // Thread used to receive message sent via UDP

    private Thread fileTransmissionThread = new FileTransmissionThread(); // Thread used to transmit files upon receiving file requests

    private Thread rereplicationThread = new RereplicationThread(); // Thread used to re-replicate files being affected upon the notification of nodes' failure

    private DatagramSocket senderUDPSocket; // UDP socket used to send message

    private DatagramSocket receiverUDPSocket; // Another UDP socket used to receive message

    private ServerSocket receiverTCPSocket; // TCP socket used to receive message

    private DatagramSocket rereplicationSocket; // Socket used to receive re-replication request from detector thread

    private Socket incomingSocket; // socket used in file transmission thread

    private int senderPort; // this node's sender port number

    private int receiverPort; // this node's receiver port number

    private int tcpReceiverPort; // this node's receiver port number (for TCP protocol)

    public static int rereplicationPort;

    private boolean runReceiver = true;

    private boolean runTcpReceiver = true;

    private boolean runRereplication = true;

    /**
     * class constructor
     *
     * @param nodeNumber: node number of current node
     * @param mode:       running mode (1 for local testing, 2 for production)
     */
    public SdfsServer(int nodeNumber, int mode) {
        this.nodeNumber = nodeNumber;

        // Initiate sender & receiver's port map
        initiatePortMap(senderPortMap, NODE_LIMIT, SENDER_PORT_START);
        initiatePortMap(receiverPortMap, NODE_LIMIT, RECEIVER_PORT_START);

        this.senderPort = senderPortMap.get(nodeNumber);
        this.receiverPort = receiverPortMap.get(nodeNumber);
        this.tcpReceiverPort = receiverPort + TCP_PORT_DIFFERENCE;
        this.rereplicationPort = receiverPort + REREPLICATION_PORT_DIFFERENCE;
        this.membershipServer = new MembershipServer(nodeNumber, nodeNumber == 1 ? 1 : 0, GOSSIP_PROTOCOL, mode); // Use gossip-style protocol for failure detection.

        // Initiate sender socket
        try {
            this.senderUDPSocket = new DatagramSocket(senderPort);
        } catch (SocketException e) {
            logger.error("SdfsServer(): fail to create sender UDP socket");
            e.printStackTrace();
        }
    }

    public void start() {
        // Start membership service
        membershipServer.startServer();

        // Start all threads
        receiverThread.start();
        fileTransmissionThread.start();
        rereplicationThread.start();
    }

    public void stop() {
        // Stop SDFS server & membership server
        membershipServer.stopServer();
        runReceiver = false;
        runTcpReceiver = false;
        runRereplication = false;

        try {
            senderUDPSocket.close();

            if (receiverUDPSocket != null)
                receiverUDPSocket.close();
            if (receiverTCPSocket != null)
                receiverTCPSocket.close();
            if (rereplicationSocket != null)
                rereplicationSocket.close();

            // Waiting for the completion of all threads
            receiverThread.join();
            fileTransmissionThread.join();
            rereplicationThread.join();

        } catch (Exception e) {
            logger.info("SdfsServer::stopServer(): Ignore exceptions that occur during server stopping. Stop the server anyway.");
        } finally {
            logger.info("SDFS server stopped");
            return;
        }
    }

    public MembershipServer getMembershipServer() {
        return membershipServer;
    }

    public HashMap<String, SdfsFile> getFullFileList() {
        return fullFileList;
    }

    public CopyOnWriteArraySet<String> getLocalFileList() {
        return localFileList;
    }

    public ArrayList<String> getFileListFromSdfsDir(String sdfsSrcDir) {
        ArrayList<String> fileList = new ArrayList<>();
        for (String sdfsFileName : fullFileList.keySet()) {
            SdfsFile sdfsFile = fullFileList.get(sdfsFileName);
            if (sdfsFile.getDirectoryPath().equals(sdfsSrcDir)) {
                fileList.add(sdfsFileName);
            }
        }
        return fileList;
    }

    // get SDFS intermediate files using prefix matching
    public ArrayList<String> getIntermediateFiles(String prefix) {
        ArrayList<String> fileList = new ArrayList<>();
        for (String sdfsFileName : fullFileList.keySet()) {
            if (sdfsFileName.startsWith(prefix)) {
                fileList.add(sdfsFileName);
            }
        }
        return fileList;
    }


    // Find all nodes' ip address where the given file is being stored (ls command)
    public void globalFinder(String sdfsFileName) {
        if (!fullFileList.containsKey(sdfsFileName)) { // The given file is not in SDFS
            System.out.println(sdfsFileName + " is not stored in the SDFS System. Please enter an existing file.");
            return;
        }
        System.out.println(String.format("%1$-15s\t%2$-20s", "Node Number", "IP Address"));
        HashSet<Integer> replicaSet = fullFileList.get(sdfsFileName).getReplicaSet();
        HashMap<Integer, String> ipMap = getIpMap();
        for (int nodeNumber : replicaSet) {
            System.out.println(String.format("%1$-15s\t%2$-20s", nodeNumber, ipMap.get(nodeNumber)));
        }
    }

    // List all files currently being stored at this machine (store command)
    public void printLocalFileList() {
        if (localFileList.size() == 0) {
            System.out.println("No SDFS File is currently stored on this machine.");
            return;
        }
        System.out.println(String.format("%1$-70s\t%2$-23s\t%3$-70s\t%4$-15s\t%5$-23s\t%6$-30s\t%7$-30s", "SDFS File Name",
                "SDFS Directory Path", "LocalFilePath", "Status", "Timestamp", "Converted Timestamp", "ReplicaSet"));

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT-5"));

        for (String sdfsFileName : localFileList) {
            SdfsFile sdfsFile = fullFileList.get(sdfsFileName);
            long timestamp = sdfsFile.getTimestamp();
            System.out.println(String.format("%1$-70s\t%2$-23s\t%3$-70s\t%4$-15s\t%5$-23s\t%6$-30s\t%7$-30s", sdfsFileName,
                    sdfsFile.getDirectoryPath(), sdfsFile.getLocalFilePath(), sdfsFile.getStatus(), timestamp, simpleDateFormat.format((new Date(timestamp))), sdfsFile.getReplicaSet()));
        }
    }

    // print all files' information stored in SDFS (for "global" command)
    public void printFullFileList() {
        if (fullFileList.size() == 0) {
            System.out.println("No SDFS File is currently stored in SDFS.");
            return;
        }
        System.out.println(String.format("%1$-70s\t%2$-23s\t%3$-70s\t%4$-15s\t%5$-23s\t%6$-30s\t%7$-30s", "SDFS File Name",
                "SDFS Directory Path", "LocalFilePath", "Status", "Timestamp", "Converted Timestamp", "ReplicaSet"));

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT-5"));

        for (String sdfsFileName : fullFileList.keySet()) {
            SdfsFile sdfsFile = fullFileList.get(sdfsFileName);
            long timestamp = sdfsFile.getTimestamp();
            System.out.println(String.format("%1$-70s\t%2$-23s\t%3$-70s\t%4$-15s\t%5$-23s\t%6$-30s\t%7$-30s", sdfsFileName,
                    sdfsFile.getDirectoryPath(), sdfsFile.getLocalFilePath(), sdfsFile.getStatus(), timestamp, simpleDateFormat.format((new Date(timestamp))), sdfsFile.getReplicaSet()));
        }
    }

    // Upload local file to SDFS (using new name specified by users) (for "put" command)
    public void putHelper(String localFileName, String sdfsFileName) {
        String[] fileInfo = extractFileInfo(sdfsFileName);
        String directoryPath = fileInfo[0];
        sdfsFileName = fileInfo[1];
        boolean isUpdate = fullFileList.containsKey(sdfsFileName);
        HashMap<Integer, String> ipMap = getIpMap();
        HashSet<Integer> replicaSet = isUpdate ? fullFileList.get(sdfsFileName).getReplicaSet() : getReplicas(sdfsFileName);
        if (replicaSet.size() != REPLICATION_NUMBER) { // Size of replica set must be at least 4 to tolerate 3 simultaneous failures
            System.out.println("[PUT_FAIL] Cannot upload the local file because there are not enough NORMAL nodes to replicate the given file. Try again later.");
            return;
        }

        logger.debug("putHelper(): replicaSet: " + replicaSet.toString() + " ; ipMap: " + ipMap.toString());

        // First ask all replica nodes whether this SDFS file can be written (avoid write & read conflict)
        int writableNum = 0, checkResp = 0;
        for (Integer replicaNodeNum : replicaSet) {
            if (replicaNodeNum != this.nodeNumber) {
                sendMsg(senderUDPSocket, ipMap.get(replicaNodeNum), receiverPortMap.get(replicaNodeNum), "[WRITE_CHECK]" + sdfsFileName);
            } else {
                ++checkResp;
                if (!isUpdate) {
                    writableNum += 1;
                } else {
                    FileStatus currentOperation = fullFileList.get(sdfsFileName).getStatus();
                    writableNum += isOperationAllowed(currentOperation, FileStatus.WRITING) ? 1 : 0; // check if the current node can write the given file.
                }
            }
        }

        // Checking response from replica. Must get write ACK from all replicas
        int abnormalNodeNum = getAbnormalNodeNum(replicaSet);
        try {
            while (checkResp < replicaSet.size() - abnormalNodeNum) {
                byte[] buffer = new byte[2048];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                senderUDPSocket.receive(packet);

                InetAddress address = packet.getAddress();
                int port = packet.getPort();
                packet = new DatagramPacket(buffer, buffer.length, address, port);
                String message = new String(packet.getData(), 0, packet.getLength()).trim();
                logger.debug("write check message: " + message);
                if ("WRITE-CHECK-ACK".equals(message)) {
                    ++writableNum;
                    ++checkResp;
                } else if ("WRITE-CHECK-NAK".equals(message)) {
                    ++checkResp;
                }

            }

            logger.debug("putHelper(): number of writable nodes: " + writableNum);
            if (writableNum < replicaSet.size() - abnormalNodeNum) {
                System.out.println("[PUT_FAIL] This SDFS file is not writable. Please try again later");
                return;
            }

            // File is writable => send write commit message to designated replica nodes.
            int commitAckNum = 0, commitResp = 0;
            for (Integer replicaNodeNum : replicaSet) {
                if (this.nodeNumber != replicaNodeNum) {
                    logger.debug("Sending write commit request to node: " + replicaNodeNum);
                    logger.debug("sending [WRITE_COMMIT]: replica set: "+ serializeSet(replicaSet));
                    sendMsg(senderUDPSocket, ipMap.get(replicaNodeNum), receiverPortMap.get(replicaNodeNum),
                            "[WRITE_COMMIT]" + this.nodeNumber + ":" + localFileName + ":" + sdfsFileName + "@" + directoryPath + "@" + serializeSet(replicaSet));
                } else if (isUpdate) { // current node is assigned as a replica
                    fullFileList.get(sdfsFileName).setTimestamp(System.currentTimeMillis());
                    fullFileList.get(sdfsFileName).setLocalFilePath(localFileName);
                    ++commitAckNum;
                    ++commitResp;
                    logger.info("SDFS File: " + sdfsFileName + " updated successfully on node: " + this.nodeNumber);
                } else {
                    SdfsFile sdfsFile = new SdfsFile(sdfsFileName, localFileName, replicaSet, directoryPath, System.currentTimeMillis());
                    fullFileList.put(sdfsFileName, sdfsFile);
                    localFileList.add(sdfsFileName);
                    ++commitAckNum;
                    ++commitResp;
                    logger.info("New file: " + sdfsFileName + " created successfully on node: " + this.nodeNumber);
                }
            }

            abnormalNodeNum = getAbnormalNodeNum(replicaSet);
            int quorum = (replicaSet.size() - abnormalNodeNum) / 2 + 1;
            boolean success = false;
            while (commitResp < replicaSet.size() - abnormalNodeNum) { // After getting quorum ACK (commit response), add file to current node's full file list.
                logger.debug("Waiting for commit response...");
                byte[] buffer = new byte[2048];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                senderUDPSocket.receive(packet);

                InetAddress address = packet.getAddress();
                int port = packet.getPort();
                packet = new DatagramPacket(buffer, buffer.length, address, port);
                String message = new String(packet.getData(), 0, packet.getLength()).trim();
                logger.debug("write commit response: " + message);
                if ("COMMIT-ACK".equals(message)) {
                    ++commitAckNum;
                    ++commitResp;
                } else if ("COMMIT-NAK".equals(message)) {
                    ++commitResp;
                }
                if (commitAckNum >= quorum) {
                    logger.info("Get acknowledgement from quorum replica nodes");
                    success = true;
                    break;
                }
            }

            if (!success) { // cannot get quorum ack from replica set
                System.out.println("[PUT_FAIL] Cannot get acknowledgement from quorum replica nodes.");
                return;
            }

            if (!replicaSet.contains(this.nodeNumber)) { // Add file entry to my full file list and local file list (if necessary)
                if (fullFileList.containsKey(sdfsFileName)) {
                    fullFileList.get(sdfsFileName).setTimestamp(System.currentTimeMillis());
                    fullFileList.get(sdfsFileName).setLocalFilePath(localFileName);
                } else {
                    fullFileList.put(sdfsFileName, new SdfsFile(sdfsFileName, localFileName, replicaSet, directoryPath, System.currentTimeMillis()));
                }
            }

            logger.info("Multicast NEW file entry to other nodes...");
            if (!isUpdate) { // If file operation is `insert`, multicast new file entry to all other nodes (exclude itself and nodes in replica set)
                SdfsFile sdfsFile = fullFileList.get(sdfsFileName);
                ArrayList<Integer> normalNodeList = membershipServer.getNormalNodeNumbers();
                for (Integer node : normalNodeList) {
                    if (!replicaSet.contains(node) && node != this.nodeNumber) {
                        logger.debug("Multicast new entry to node: " + node);
                        sendMsg(senderUDPSocket, ipMap.get(node), receiverPortMap.get(node), "[MULTICAST_INSERT]" + serializeSdfsFile(sdfsFile));
                    }
                }
            }

            logger.info("[PUT_SUCCESS] Add/Update SDFS file: " + sdfsFileName + " by using local file: " + localFileName + " successfully");
            System.out.println("[PUT_SUCCESS] Add/Update SDFS file: " + sdfsFileName + " by using local file: " + localFileName + " successfully");
        } catch (IOException e) {
            logger.error("putHelper(): IO Exception thrown");
            e.printStackTrace();
        }
    }

    // Delete an SDFS file from the SDFS system.
    public void deleteHelper(String sdfsFileName) {
        if (!fullFileList.containsKey(sdfsFileName)) {
            System.out.println("[DELETE_FAIL] " + sdfsFileName + " is not stored in the SDFS system. Please try deleting an existing file.");
            return;
        }

        SdfsFile sdfsFile = fullFileList.get(sdfsFileName);
        HashSet<Integer> replicaSet = sdfsFile.getReplicaSet();
        HashMap<Integer, String> ipMap = getIpMap();

        // send delete-check to each replica and check the file status
        int checkResp = 0, deleteAckNum = 0;
        for (int replicaNodeNum : replicaSet) {
            if (replicaNodeNum != this.nodeNumber) {
                sendMsg(senderUDPSocket, ipMap.get(replicaNodeNum), receiverPortMap.get(replicaNodeNum), "[DELETE_CHECK]" + sdfsFileName);
            }
        }

        if (replicaSet.contains(this.nodeNumber)) {
            ++checkResp;
            deleteAckNum += isOperationAllowed(sdfsFile.getStatus(), FileStatus.WRITING) ? 1 : 0;
        }

        // Get delete check response from replica nodes (excluding itself)
        try {
            // collect delete-ack from each replica node
            int abnormalNodeNum = getAbnormalNodeNum(replicaSet);
            while (checkResp < replicaSet.size() - abnormalNodeNum) {
                byte[] buffer = new byte[2048];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                senderUDPSocket.receive(packet);

                InetAddress address = packet.getAddress();
                int port = packet.getPort();
                packet = new DatagramPacket(buffer, buffer.length, address, port);
                String message = new String(packet.getData(), 0, packet.getLength()).trim();
                logger.debug("delete check message: " + message);
                if ("DELETE-ACK".equals(message)) {
                    ++deleteAckNum;
                    ++checkResp;
                } else if ("DELETE-NAK".equals(message)) {
                    ++checkResp;
                }
            }

            logger.debug("deleteHelper(): number of delete acks: " + deleteAckNum);
            // if not all replicas are deletable, return.
            if (deleteAckNum < replicaSet.size() - abnormalNodeNum) {
                System.out.println("[DELETE_FAIL] This SDFS file cannot be deleted since its status is reading/writing. Please try again later");
                return;
            }
            // multicast the file deletion to all other normal nodes in the group.
            ArrayList<Integer> normalNodeList = membershipServer.getNormalNodeNumbers();
            for (Integer node : normalNodeList) {
                if (node != this.nodeNumber) {
                    logger.debug("Multicast a delete message to node: " + node);
                    sendMsg(senderUDPSocket, ipMap.get(node), receiverPortMap.get(node), "[MULTICAST_DELETE]" + sdfsFileName);
                } else {
                    deleteFile(sdfsFileName);  // remove the file from both local and full file list.
                }
            }

            logger.info("[DELETE_SUCCESS] Delete SDFS file: " + sdfsFileName + " successfully.");
            System.out.println("[DELETE_SUCCESS] Delete SDFS file: " + sdfsFileName + " successfully.");
        } catch (IOException e) {
            logger.error("deleteHelper(): IO Exception thrown");
            e.printStackTrace();
        }
    }

    // Fetch an SDFSfile to local dir
    public void getHelper(String sdfsFileName, String localFileName) {
        if (!fullFileList.containsKey(sdfsFileName)) {
            System.out.println("[GET_FAIL] " + sdfsFileName + " is not stored in the SDFS system. Please enter an existing file.");
            return;
        }
        HashSet<Integer> replicaSet = fullFileList.get(sdfsFileName).getReplicaSet();
        HashMap<Integer, String> ipMap = getIpMap();
        int readableNum = 0, checkResp = 0;
        for (Integer replicaNodeNum : replicaSet) {
            if (replicaNodeNum != this.nodeNumber) {
                sendMsg(senderUDPSocket, ipMap.get(replicaNodeNum), receiverPortMap.get(replicaNodeNum), "[READ_CHECK]" + sdfsFileName);
            } else {
                ++checkResp;
                FileStatus currentOperation = fullFileList.get(sdfsFileName).getStatus();
                readableNum += isOperationAllowed(currentOperation, FileStatus.READING) ? 1 : 0; // check if the current node can read the given file.
            }
        }

        // check the sdfs file status. Cannot read a file while the file is being written.
        if (replicaSet.contains(this.nodeNumber) && readableNum < 1) {
            System.out.println("[GET_FAIL] This SDFS file is not readable. Please try again later");
            return;
        }
        try {

            // count the number of READ-CHECK-ACK received to check whether the SDFS file is readable
            int abnormalNodeNum = getAbnormalNodeNum(replicaSet);
            while (checkResp < replicaSet.size() - abnormalNodeNum) {
                byte[] buffer = new byte[2048];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                senderUDPSocket.receive(packet);

                InetAddress address = packet.getAddress();
                int port = packet.getPort();
                packet = new DatagramPacket(buffer, buffer.length, address, port);
                String message = new String(packet.getData(), 0, packet.getLength()).trim();
                logger.debug("read check message: " + message);

                if ("READ-CHECK-ACK".equals(message)) {
                    ++readableNum;
                    ++checkResp;
                } else if ("READ-CHECK-NAK".equals(message)) {
                    ++checkResp;
                    break;
                }
            }

            logger.debug("getHelper(): number of readable nodes: " + readableNum);
            // return if the SDFS file is not readable
            if (readableNum < replicaSet.size() - abnormalNodeNum) {
                System.out.println("[GET_FAIL] This SDFS file is not readable. Please try again later");
                return;
            }

            // set the quorum to be replicaSet.size/2 + 1, only read from quorum number of files.
            int quorum = (replicaSet.size() - abnormalNodeNum) / 2 + 1, tsResp = 0;
            for (int replicaNodeNum : replicaSet) {
                if (this.nodeNumber != replicaNodeNum) {
                    sendMsg(senderUDPSocket, ipMap.get(replicaNodeNum), receiverPortMap.get(replicaNodeNum), "[TS]" + sdfsFileName);
                }
            }

            long latestTS = -1;
            int latestNodeNum = -1;
            String localFilePath = "";
            if (replicaSet.contains(this.nodeNumber)) {
                tsResp++;
                latestTS = fullFileList.get(sdfsFileName).getTimestamp();
                latestNodeNum = this.nodeNumber;
                localFilePath = fullFileList.get(sdfsFileName).getLocalFilePath();
            }

            // take quorum number of responses
            while (tsResp < quorum) {
                byte[] buffer = new byte[2048];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                senderUDPSocket.receive(packet);
                InetAddress address = packet.getAddress();
                int port = packet.getPort();
                packet = new DatagramPacket(buffer, buffer.length, address, port);
                String message = new String(packet.getData(), 0, packet.getLength()).trim();
                logger.debug("read ts-resp message: " + message);
                if (message.startsWith("[TS-RESP]")) {
                    String[] resp = message.substring("[TS-RESP]".length()).split("@");
                    long ts = Long.parseLong(resp[0]);
                    // only read the file with the latest timestamp
                    if (ts > latestTS) {
                        latestTS = ts;
                        latestNodeNum = Integer.parseInt(resp[1]);
                        localFilePath = resp[2];
                    }
                    ++tsResp;
                }
            }

            logger.debug("Requesting SDFS file: " + sdfsFileName + " from node " + latestNodeNum + " (local path: " + localFilePath + " )");
            //initiate the file request
            fileRequest(ipMap.get(latestNodeNum), receiverPortMap.get(latestNodeNum) + TCP_PORT_DIFFERENCE, localFilePath, localFileName, false);

            logger.info("[GET_SUCCESS] GET SDFS file: " + sdfsFileName + " successfully.");
            System.out.println("[GET_SUCCESS] GET SDFS file: " + sdfsFileName + " successfully.");
        } catch (IOException e) {
            logger.error("getHelper(): IO Exception thrown");
            e.printStackTrace();
        }
    }

    private int getAbnormalNodeNum(HashSet<Integer> replicaSet) {
        int abnormalNodeNum = 0;
        for (Integer nodeId : replicaSet) {
            if (!membershipServer.isNodeNormal(nodeId)) {
                ++abnormalNodeNum;
            }
        }
        return abnormalNodeNum;
    }

    // delete a file from fullFileList and localFileList
    private void deleteFile(String sdfsFileName) {
        fullFileList.remove(sdfsFileName);
        if (localFileList.contains(sdfsFileName)) {
            localFileList.remove(sdfsFileName);
            logger.info("At Node " + this.nodeNumber + " : delete SDFS file: " + sdfsFileName + " from local file list.");
        }
    }

    // Get Replica set. Set the size of replica set to 4, so that the system can tolerate up to 3 failures.
    private HashSet<Integer> getReplicas(String sdfsFileName) {
        ArrayList<Integer> normalNodes = membershipServer.getNormalNodeNumbers();
        HashSet<Integer> replicaSet = new HashSet<>();
        if (normalNodes.size() < REPLICATION_NUMBER) {
            return replicaSet;
        }

        // Pick the initial replica node by hashing the filename, then pick the rest of nodes in clockwise order.
        int startNode = hashFile(sdfsFileName, normalNodes.size());
        logger.debug("getReplicas(): file start node: " + startNode + ", normalNodeNumbers: " + normalNodes.toString());

        for (int num : normalNodes) {
            if (num >= startNode) {
                replicaSet.add(num);
                if (replicaSet.size() == REPLICATION_NUMBER) {
                    return replicaSet;
                }
            }
        }
        for (Integer i : normalNodes) {
            if (replicaSet.size() < REPLICATION_NUMBER) {
                replicaSet.add(i);
            } else {
                break;
            }
        }
        return replicaSet;
    }

    // Get the IP map indicating the IP address of each node. Key: Node Number, Value: IP address.
    public HashMap<Integer, String> getIpMap() {
        HashMap<Integer, String> ipMap = new HashMap<>();
        ipMap.put(this.nodeNumber, membershipServer.node.getIpAddr());

        for (String nodeId : membershipServer.node.getMembers().keySet()) {
            String[] info = nodeId.split("@");
            int nodeNumber = Integer.parseInt(info[0]);
            String ip = info[1];
            ipMap.put(nodeNumber, ip);
        }
        return ipMap;
    }

    private void updateFileList(String localFileName, String sdfsFileStr, int sourceNode, String sourceIp, boolean isRereplicate) {
        logger.debug("local file name: " + localFileName + "; sdfsFileStr: " + sdfsFileStr);
        String[] info = sdfsFileStr.split("@", 0);
        String sdfsFileName = info[0];
        String directoryPath = info[1];
        logger.debug("updateFileList(): sdfsFileStr: "+ sdfsFileStr);
        logger.debug("updateFileList(): replicaSet: "+ info[2]);

        HashSet<Integer> replicaSet = deserializeSet(info[2]);

        boolean isUpdate = fullFileList.containsKey(sdfsFileName);
        if (isUpdate && !isRereplicate) { // Update an SDFS's information
            fullFileList.get(sdfsFileName).setStatus(FileStatus.WRITING);
            fullFileList.get(sdfsFileName).setLocalFilePath(sdfsFileName + "-" + this.nodeNumber);
            fullFileList.get(sdfsFileName).setTimestamp(System.currentTimeMillis());
            fullFileList.get(sdfsFileName).setReplicaSet(replicaSet);
        } else { // Insert new SDFS file
            fullFileList.put(sdfsFileName, new SdfsFile(sdfsFileName, sdfsFileName + "-" + this.nodeNumber, replicaSet, directoryPath, System.currentTimeMillis(), FileStatus.WRITING));
            localFileList.add(sdfsFileName);
        }
        logger.debug(isRereplicate ? "Sending file request for re-replication..." : "Sending file request for inserting/updating...");
        // Send request to the source node asking for file
        fileRequest(sourceIp, receiverPortMap.get(sourceNode) + TCP_PORT_DIFFERENCE, localFileName, sdfsFileName + "-" + this.nodeNumber, false);
        fullFileList.get(sdfsFileName).setStatus(FileStatus.NOOP);
    }

    // Send re-replicate request to a newly picked replica node
    private void sendRereplicateReq(int newReplicaNode, String localFilePath, String sdfsFileName, HashSet<Integer> replicaSet, String directoryPath) {
        HashMap<Integer, String> ipMap = getIpMap();
        logger.info("Sending re-replication request to node: " + newReplicaNode + " for replicating SDFS file: " + sdfsFileName);
        sendMsg(senderUDPSocket, ipMap.get(newReplicaNode), receiverPortMap.get(newReplicaNode), "[REREPLICATE]" + this.nodeNumber + ":" + localFilePath + ":" + sdfsFileName + "@" + directoryPath + "@" + serializeSet(replicaSet));
    }

    // Thread used to receive messages (except file transmission messages)
    private class ReceiverThread extends Thread {
        @Override
        public void run() {
            try {
                receiverUDPSocket = new DatagramSocket(receiverPort);

                while (runReceiver) {
                    byte[] buffer = new byte[2048];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                    receiverUDPSocket.receive(packet);
                    if (membershipServer.node.getStatus() != 1) { // Node left => do nothing
                        continue;
                    }

                    InetAddress address = packet.getAddress();
                    String sourceIp = address.getHostAddress();
                    int port = packet.getPort();
                    packet = new DatagramPacket(buffer, buffer.length, address, port);

                    String message = new String(packet.getData(), 0, packet.getLength()).trim();
                    logger.debug("ReceiverThread():: receive message: " + message);

                    if (message.startsWith("[WRITE_CHECK]")) { // Check if the given file can be written (avoid write & read conflict)
                        String fileName = message.substring("[WRITE_CHECK]".length());
                        boolean isWritable = true;
                        if (fullFileList.containsKey(fileName)) {
                            isWritable = isOperationAllowed(fullFileList.get(fileName).getStatus(), FileStatus.WRITING);
                        }
                        logger.debug("ReceiverThread():: " + fileName + (isWritable ? " writable" : " not writable"));
                        sendMsg(senderUDPSocket, sourceIp, port, (isWritable ? "WRITE-CHECK-ACK" : "WRITE-CHECK-NAK"));
                    } else if (message.startsWith("[DELETE_CHECK]")) { // Check if the given file can be deleted
                        String fileName = message.substring("[DELETE_CHECK]".length());
                        boolean isDeletable = false;
                        if (fullFileList.containsKey(fileName)) {
                            isDeletable = isOperationAllowed(fullFileList.get(fileName).getStatus(), FileStatus.WRITING); // Deleting is equivalent to writing.
                        }
                        logger.debug("ReceiverThread():: " + fileName + (isDeletable ? " deletable" : " not deletable"));
                        sendMsg(senderUDPSocket, sourceIp, port, (isDeletable ? "DELETE-ACK" : "DELETE-NAK"));
                    } else if (message.startsWith("[READ_CHECK]")) { // Check if the given file is readable
                        String fileName = message.substring("[READ_CHECK]".length());
                        boolean isReadable = false;
                        if (fullFileList.containsKey(fileName)) {
                            isReadable = isOperationAllowed(fullFileList.get(fileName).getStatus(), FileStatus.READING); // Deleting is equivalent to writing.
                        }
                        logger.debug("ReceiverThread():: " + fileName + (isReadable ? " Readable" : " not readalbe"));
                        sendMsg(senderUDPSocket, sourceIp, port, (isReadable ? "READ-CHECK-ACK" : "READ-CHECK-NAK"));
                    } else if (message.startsWith("[TS]")) { // Retrieve the latest timestamp of the given file. Also send back this SDFS file's local file path
                        String fileName = message.substring("[TS]".length());
                        long ts = fullFileList.get(fileName).getTimestamp();
                        String localFilePath = fullFileList.get(fileName).getLocalFilePath();
                        sendMsg(senderUDPSocket, sourceIp, port, "[TS-RESP]" + ts + "@" + nodeNumber + "@" + localFilePath);
                    } else if (message.startsWith("[WRITE_COMMIT]")) { // Node which receives this message is assigned as a replica of an SDFS file.
                        String[] info = message.substring("[WRITE_COMMIT]".length()).split(":", 0);
                        int sourceNode = Integer.parseInt(info[0]);
                        String localFileName = info[1], sdfsFileStr = info[2];
                        updateFileList(localFileName, sdfsFileStr, sourceNode, sourceIp, false);

                        logger.debug("Sending commit resp...");
                        sendMsg(senderUDPSocket, sourceIp, port, "COMMIT-ACK");
                    } else if (message.startsWith("[REREPLICATE]")) { // Node which receives this message is assigned as a new replica of an SDFS file
                        logger.info("Start re-replication...");
                        String[] fileInfoArr = message.substring("[REREPLICATE]".length()).split(":");
                        Integer sourceNode = Integer.parseInt(fileInfoArr[0]);
                        String localFilePath = fileInfoArr[1], sdfsFileStr = fileInfoArr[2];

                        updateFileList(localFilePath, sdfsFileStr, sourceNode, sourceIp, true);

                        // Multicast new replica set to other nodes (excluding source node)
                        ArrayList<Integer> normalNodeList = membershipServer.getNormalNodeNumbers();
                        HashMap<Integer, String> ipMap = getIpMap();
                        for (Integer node : normalNodeList) {
                            if (node != nodeNumber && node != sourceNode) {
                                logger.debug("After re-replication, multicast new replica set to node: " + node);
                                sendMsg(senderUDPSocket, ipMap.get(node), receiverPortMap.get(node), "[MULTICAST_REPLICA]" + sdfsFileStr);
                            }
                        }
                        logger.info("Re-replication done!");
                    } else if (message.startsWith("[MULTICAST_INSERT]")) { // Get new file's insertion message. Add the file to full file list
                        String sdfsFileStr = message.substring("[MULTICAST_INSERT]".length());
                        logger.debug("[MULTICAST_INSERT] sdfsFileStr: " + sdfsFileStr);
                        SdfsFile sdfsFile = deserializeSdfsFileStr(sdfsFileStr);

                        fullFileList.put(sdfsFile.getSdfsFileName(), sdfsFile);
                        logger.info("GET multicast insert => add new SDFS file: " + sdfsFile.getSdfsFileName() + " to my full file list...");
                    } else if (message.startsWith("[MULTICAST_DELETE]")) { // Get delete message => remove the given file from full file list.
                        String sdfsFileName = message.substring("[MULTICAST_DELETE]".length());
                        deleteFile(sdfsFileName);
                        logger.info("GET multicast delete => delete file: " + sdfsFileName + " from SDFS.");
                    } else if (message.startsWith("[MULTICAST_REPLICA]")) { // Get replicas update message => update the replica set of the given SDFS file
                        String sdfsFileStr = message.substring("[MULTICAST_REPLICA]".length());
                        String[] info = sdfsFileStr.split("@", 0);
                        String sdfsFileName = info[0];
                        HashSet<Integer> replicaSet = deserializeSet(info[2]);

                        fullFileList.get(sdfsFileName).setReplicaSet(replicaSet);
                    }
                }
            } catch (SocketException e) {
                logger.info("Closing receiver socket");
            } catch (IOException e) {
                logger.error("ReceiverThread::run(): exception thrown");
                e.printStackTrace();
            } finally { // finally close the socket
                if (receiverUDPSocket != null) {
                    receiverUDPSocket.close();
                }
                logger.info("Receiver socket closed");
            }
        }
    }

    // Thread used to transmit files
    private class FileTransmissionThread extends Thread {
        @Override
        public void run() {
            try {
                receiverTCPSocket = new ServerSocket(tcpReceiverPort);
                PrintWriter out = null;
                BufferedReader in = null;

                while (runTcpReceiver) {
                    incomingSocket = receiverTCPSocket.accept();
                    out = new PrintWriter(incomingSocket.getOutputStream(), true);
                    in = new BufferedReader(new InputStreamReader(incomingSocket.getInputStream()));

                    String message = in.readLine();
                    if (message.startsWith("[FILE_REQ]")) {
                        String localFileName = message.substring("[FILE_REQ]".length());

                        logger.info("FileTransmissionThread: received a request to transmit file: " + localFileName);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(localFileName)));
                        char[] buf = new char[2048];

                        // read file by chunks to reduce memory usage
                        int charRead = reader.read(buf, 0, buf.length);
                        while (charRead != -1) { // Send file by chunk
                            out.write(new String(buf, 0, charRead));
                            charRead = reader.read(buf, 0, buf.length);
                        }
                        out.close();
                    }
                }
                logger.info("FileTransmissionThread: Closing receiver TCP Socket...");
                if (in != null) in.close();
                if (out != null) out.close();
                if (incomingSocket != null) incomingSocket.close();
                receiverTCPSocket.close();

            } catch (IOException e) {
                logger.info("FileTransmissionThread: error occurs. Ignored.");
//                e.printStackTrace();
            }
        }
    }

    // Thread used to re-replicate file upon certain node's failure
    private class RereplicationThread extends Thread {
        @Override
        public void run() {
            try {
                rereplicationSocket = new DatagramSocket(rereplicationPort);

                while (runRereplication) {
                    byte[] buffer = new byte[2048];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                    rereplicationSocket.receive(packet);

                    InetAddress address = packet.getAddress();
                    String sourceIp = address.getHostAddress();
                    int port = packet.getPort();
                    packet = new DatagramPacket(buffer, buffer.length, address, port);

                    int failedNodeId = Integer.parseInt(new String(packet.getData(), 0, packet.getLength()).trim());
                    logger.info("Re-replicationThread():: failed node: " + failedNodeId);

                    for (String sdfsFileName : localFileList) {
                        SdfsFile sdfsFile = fullFileList.get(sdfsFileName);
                        HashSet<Integer> replicaSet = sdfsFile.getReplicaSet();

                        // This node is responsible for re-replicating the file stored on the failed node, because it is the smallest node in replica set.
                        if (replicaSet.remove(failedNodeId) && nodeNumber == Collections.min(replicaSet)) {
                            HashSet<Integer> normalNodeSet = membershipServer.getNormalNodeNumbers(true);
                            normalNodeSet.removeAll(replicaSet);

                            //randomly pick a normal non-replica node as the new replica node
                            if (normalNodeSet.size() > 0) {
                                int newReplicaNode = -1, randomN = new Random().nextInt(normalNodeSet.size()), i = 0;
                                for (Integer nodeId : normalNodeSet) {
                                    if (i == randomN) {
                                        newReplicaNode = nodeId;
                                    }
                                    ++i;
                                }
                                replicaSet.add(newReplicaNode);
                                logger.info("Add new node: " + newReplicaNode + " to file: " + sdfsFileName + " 's replica set");
                                //send replica request to the new replica node
                                sendRereplicateReq(newReplicaNode, sdfsFile.getLocalFilePath(), sdfsFile.getSdfsFileName(), replicaSet, sdfsFile.getDirectoryPath());
                            } else {
                                logger.warn("There is not enough node to replicate file.");
                            }
                        }
                    }
                }
            } catch (SocketException e) {
                logger.info("Closing re-replication socket");
            } catch (IOException e) {
                logger.error("Re-replicationThread::run(): exception thrown");
                e.printStackTrace();
            } finally { // finally close the socket
                if (rereplicationSocket != null) {
                    rereplicationSocket.close();
                }
                logger.info("Re-replication socket closed");
            }

        }
    }
}
