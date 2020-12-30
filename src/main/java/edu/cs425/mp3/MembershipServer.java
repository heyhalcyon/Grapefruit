package edu.cs425.mp3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;

import static edu.cs425.mp3.Utility.*;

class MembershipServer {

    private static final HashMap<Integer, Integer> senderPortMap = new HashMap<Integer, Integer>(); // map node number to its sender port number

    private static final HashMap<Integer, Integer> receiverPortMap = new HashMap<Integer, Integer>(); // map node number to its receiver port number

    private final HashMap<Integer, String> statusMap = new HashMap<Integer, String>() {{ // map status number to text description
        put(0, "Fail");
        put(1, "Normal");
        put(2, "Leave");
    }};

    private int SENDER_PORT_START = 4441;

    private int RECEIVER_PORT_START = 5551;

    private int NODE_LIMIT = 10;

    private final long THREAD_TIME_OUT = 2000L; // Switch protocol after this time period

    private final String INTRODUCER_PREFIX = "1@"; // Prefix of Introducer Id

    private final double SAMPLE_RATIO = 0.6; // Sample ratio used in Gossip failure detection

    public int nodeNumber; // Number of node, range: [1, 10]

    private static int senderPort; // Number of port used for sending message

    private static int receiverPort; // Number of port used for receiving message

    private static final int INTRODUCER_PORT = 9999; //Introducer port is fixed

    private static String INTRODUCER_IP; // Introducer IP: "localhost" for local testing, "fa20-cs425-g31-01.cs.illinois.edu" for remote testing.

    private final long HB_PERIOD = 500L; // Heartbeat period

    private final long T_FAIL_ALL2ALL = 2000L; // T_fail for All-to-All protocol

    private final long T_FAIL_GOSSIP = 2000L; // T_fail for Gossip protocol

    private static final Logger logger = LogManager.getLogger(MembershipServer.class); // logger instance used for logging (provided by log4j2 library)

    private DatagramSocket socket; // socket used especially for send heartbeat and new node join request

    private DatagramSocket receiverSocket = null; // socket used for receiving heartbeat

    private DatagramSocket introducerSocket = null; // Socket on introducer used to listen join request (bond to a fixed port: 8888)

    public Node node; // Node class.

    public boolean isIntroducer = false; // Indicate whether current process is an introducer process or not.

    private boolean runIntroducer = false; // To control IntroducerThread

    private boolean runReceiver = true; // To control ReceiverThread

    private boolean runHeartBeat = true; // To control HeartbeatThread

    private boolean runDetector = true; // To control DetectorThread

    public int protocol;  // 1 for all-to-all, 2 for gossip style

    public String ipAddr; // IP address of current vm

//    private Thread handleUIThread = new HandleUIThread(); // Thread used for handling user input

    private Thread hbThread = new HbThread(); // Thread used for sending heartbeats

    private Thread receiverThread = new ReceiverThread(); // Thread used for receiving message (Heartbeats, leave,

    private Thread detectorThread = new DetectorThread(); // Thread used for failure detection

    private Thread introducerThread = new IntroducerThread(); // Thread used for serving join request (only run on Introducer process)

    private static int cumulatedBytesIn = 0; // For experiment

    private static int cumulatedBytesOut = 0; // For experiment

    private static int lostHbNum = 0; // For experiment

    // Default constructor
    public MembershipServer() {
    }

    // Constructor
    public MembershipServer(int nodeNumber, int introducer, int protocol, int mode) {

        try {
            this.nodeNumber = nodeNumber;

            initiatePortMap(senderPortMap, NODE_LIMIT, SENDER_PORT_START);
            initiatePortMap(receiverPortMap, NODE_LIMIT, RECEIVER_PORT_START);

            senderPort = senderPortMap.get(nodeNumber);
            receiverPort = receiverPortMap.get(nodeNumber);
            INTRODUCER_IP = (mode == 1 ? "localhost" : "fa20-cs425-g31-01.cs.illinois.edu"); // In production, always use vm 01 as the introducer.

            socket = new DatagramSocket(senderPort); // socket used to send message

            InetAddress localhost = InetAddress.getLocalHost();
            ipAddr = localhost.getHostAddress().trim();

            this.node = new Node(nodeNumber, ipAddr, System.currentTimeMillis()); // initial status = 1(NORMAL)

            this.protocol = protocol;
            if (introducer == 1) {
                setIsIntroducer(true);
                setRunIntroducer(true);
            }

        } catch (UnknownHostException e) {
            logger.error("Server constructor: UnknownHostException (localhost name could not be resolved into an address.");
            e.printStackTrace();
        } catch (SocketException e) {
            logger.error("Server constructor: fail to create socket");
            e.printStackTrace();
        }
    }

    // Start the server
    public void startServer() {
        // Start all threads
        // handleUIThread.start();
        hbThread.start();
        receiverThread.start();
        detectorThread.start();
        if (isIntroducer) {
            introducerThread.start();
        }
    }

    // Stop the server
    public void stopServer() {
        // Stop all threads
        runDetector = false;
        runHeartBeat = false;
        runReceiver = false;
        if (isIntroducer) {
            runIntroducer = false;
        }

        try {
            // Close all sockets
            socket.close();
            receiverSocket.close();
            if (isIntroducer) {
                introducerSocket.close();
            }

            detectorThread.join();
            hbThread.join();
            receiverThread.join();
            if (introducerThread != null) {
                introducerThread.join();
            }
        } catch (Exception e) {
            logger.info("MembershipServer::stopServer(): Ignore exceptions that occur during server stopping. Stop the server anyway.");
        } finally {
            logger.info("Membership Server stopped");
            return;
        }
    }

    // Setter
    public void setIsIntroducer(boolean introducer) {
        isIntroducer = introducer;
    }

    // Setter
    public void setRunIntroducer(boolean runIntroducer) {
        this.runIntroducer = runIntroducer;
    }

    // Print the node information
    public void printNodeInfo() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT-5"));
        Date convertedTs = new Date(node.getCreatedTs());
        System.out.println(String.format("%1$-33s\t%2$-20s\t%3$-24s\t%4$-24s\t%5$-5s", "Id", "Heartbeat Counter", "Created Unix Timestamp", "Converted Created Timestamp", "Status"));
        System.out.println(String.format("%1$-33s\t%2$-20s\t%3$-24s\t%4$-24s\t%5$-5s", node.getId(), node.getHbCounter(), node.getCreatedTs(), simpleDateFormat.format(convertedTs), statusMap.get(node.getStatus())));
    }

    // Print the membership list info
    public void printMembershipList() {
        if (!isIntroducer && node.getMembers().size() == 0) { // There is no member in the group.
            System.out.println("There is no member in the group. Try joining the group first.");
            return;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT-5"));

        System.out.println(String.format("%1$-33s\t%2$-20s\t%3$-24s\t%4$-20s\t%5$-5s", "Id", "Hearbeat Counter", "Latest Timestamp", "Converted Timestamp", "Status"));
        System.out.println(String.format("%1$-33s\t%2$-20s\t%3$-24s\t%4$-20s\t%5$-5s", node.getId(), node.getHbCounter(), node.getCurTs(), simpleDateFormat.format(new Date(node.getCurTs())), statusMap.get(node.getStatus())));
        for (String id : node.getMembers().keySet()) {
            ArrayList<String> infoArr = node.getMembers().get(id);
            System.out.println(String.format("%1$-33s\t%2$-20s\t%3$-24s\t%4$-20s\t%5$-5s", id, infoArr.get(0), infoArr.get(1), simpleDateFormat.format((new Date(Long.parseLong(infoArr.get(1))))), statusMap.get(Integer.parseInt(infoArr.get(2)))));
        }
    }

    // Parse node number from a node id
    public int getNodeNumberFromId(String id) {
        return Integer.parseInt(id.split("@", 0)[0]);
    }

    // Parse IP Address from a node id
    public String getIpAddrFromId(String id) {
        return id.split("@", 0)[1];
    }

    // Get normal nodes from the membership list, return a list of sorted node ids
    public ArrayList<String> getNormalNodes(boolean includeIntroducer) {
        ArrayList<String> activeList = new ArrayList<>();
        for (String curId : node.getMembers().keySet()) {
            if (node.getMembers().get(curId).get(2).equals("1")) {
                activeList.add(curId);
            }
        }
        if (includeIntroducer) {
            activeList.add("1@" + INTRODUCER_IP + "@xxx"); // "@xxx" is only used for aligning the format. Timestamp is not needed in sending message.
        }
        return activeList;
    }

    public ArrayList<Integer> getNormalNodeNumbers() {
        ArrayList<Integer> activeList = new ArrayList<>();
        for (String curId : node.getMembers().keySet()) {
            if (node.getMembers().get(curId).get(2).equals("1")) {
                String[] info = curId.split("@");
                activeList.add(Integer.parseInt(info[0]));
            }
        }
        if (node.getStatus() == 1) {
            activeList.add(nodeNumber);
        }
        Collections.sort(activeList);
        return activeList;
    }

    public HashSet<Integer> getNormalNodeNumbers(Boolean getSet) {
        HashSet<Integer> activeSet = new HashSet<>();
        for (String curId : node.getMembers().keySet()) {
            if (node.getMembers().get(curId).get(2).equals("1")) {
                String[] info = curId.split("@");
                activeSet.add(Integer.parseInt(info[0]));
            }
        }
        if (node.getStatus() == 1) {
            activeSet.add(nodeNumber);
        }
        return activeSet;
    }

    // Send the join request to Introducer, and receive the membership list from Introducer
    public int joinHelper() {
        String nodeId = node.getId();
        try {
            //send join request to Introducer
            int resp = sendMsg(socket, INTRODUCER_IP, INTRODUCER_PORT, nodeId);
            if (resp == -1) {
                return -1; // sendMsg failed
            }

            // Timeout if sender socket does not receive any response from introducer (throw SocketTimeoutException).
            socket.setSoTimeout(500);

            byte[] buffer = new byte[2048];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            socket.receive(packet);

            InetAddress address = packet.getAddress();
            int port = packet.getPort();
            packet = new DatagramPacket(buffer, buffer.length, address, port);
            String message = new String(packet.getData(), 0, packet.getLength()).trim();

            // Deserialize the node info from the message (format: Java String) received from Introducer
            HashMap<String, ArrayList<String>> members = deserialize(message.substring(6)); // remove prefix: "[LIST]"
            members.remove(node.getId()); // remove itself from members
            node.setCurTs(System.currentTimeMillis());
            node.setMembers(members); // add nodes in the group to my membership list
            logger.info("Node (Id: " + nodeId + ") successfully joins the group and receives membership list from introducer");

            return resp; // join request succeeded

        } catch (SocketTimeoutException e) {
            logger.info("joinHelper(): No response received from Introducer (time out)");
            return -1;
        } catch (IOException e) {
            logger.error("joinHelper(): IO Exception thrown");
            e.printStackTrace();
            return -1;
        }
    }

    // Send leave message to a sample of nodes in the membership list
    public void leaveHelper() {
        node.setStatus(2);

        ArrayList<String> activeList = getNormalNodes(false); // Will not include introducer in sender list
        Set<String> gossipList = getGossipList(activeList); // Get a sample of normal nodes to send the leave message to

        for (String curId : gossipList) {
            logger.info("LeaveHelper(): node:(id=" + node.getId() + ") + notifies Node:(id" + curId + ") that it has left the group");
            sendMsg(socket, getIpAddrFromId(curId), receiverPortMap.get(getNodeNumberFromId(curId)), "[LEAVE]" + node.getId());
        }
        // Clear my membership list when leaving
        node.getMembers().clear();
        logger.info("Node (ID: " + node.getId() + ") left the group.");
    }

    // For experiment (some hard coding involved since the code is only for experiment)
    private void lostHelper(int nodeNum, int lostNum) {
        ArrayList<String> normalIds = getNormalNodes(false);
        int targetNum = nodeNum - 1;

        for (int i = 0; i < targetNum; i++) {
            String id = normalIds.get(i);
            sendMsg(socket, getIpAddrFromId(id), receiverPortMap.get(getNodeNumberFromId(id)), "[LOST]" + lostNum);
        }
        try {
            Thread.sleep(1000L);
            lostHbNum = lostNum;
            logger.info("Current node lost " + lostNum + " HB in each HB period");

            Thread.sleep(4000L);
            printMembershipList();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("lostHelper(): InterruptionException is thrown");
        }
    }

    // After receiving a heartbeat, merge other nodes' membership list into my membership list
    private void mergeMembers(String[] members, int protocol) {
        Long curTimestamp = System.currentTimeMillis();
        String hbSourceId = members[0].split(",", 0)[0];

        for (String member : members) {
            String[] info1 = member.split(",", 0);
            String id1 = info1[0];
            int hb1 = Integer.parseInt(info1[1]), s1 = Integer.parseInt(info1[2]);

            if (id1.equals(node.getId())) { // the node in current iteration is the current vm itself, no need to update
                continue;
            }
            if (node.getMembers().containsKey(id1)) { // get information of node which is also in my membership list

                ArrayList<String> info2 = node.getMembers().get(id1);
                int hb2 = Integer.parseInt(info2.get(0)), s2 = Integer.parseInt(info2.get(2));

                if (s1 == 1 && s2 == 1 && hb1 > hb2) { // Update node heartbeat counter and latest timestamp to current timestamp
//                    if (protocol == 2 || (protocol == 1 && id1.equals(hbSourceId))) { // protocol = 1 (all-to-all), 2 (gossip)
                    info2.set(0, Integer.toString(hb1));
                    info2.set(1, Long.toString(curTimestamp));
                    continue;
//                    }
                }
                if (s1 == 0 && s2 != 0) { // the node is marked failed in another node's membership list. Also mark it failed in my list.
                    updateNodeStatusInList(id1, 0);
                    notifyFailure(id1);
                    logger.info(String.format("Node (Id: " + hbSourceId + ")'s membership list shows node (Id: %1$s) failed. Mark its status as failed in my list.", id1));
                    continue;
                }
                if (s1 == 2 && s2 == 1) { // the node is marked "leave" in another node's membership list. Also mark it "leave" in my list.
                    updateNodeStatusInList(id1, 2);
                    notifyFailure(id1);
                    logger.info(String.format("Node (Id: " + hbSourceId + ")'s membership list shows node (Id: %1$s) left. Mark its status as left in my list.", id1));
                    continue;
                }
            } else { // get information of node that is not in my membership list. Add it to my list.
                logger.info("Node (Id: " + hbSourceId + ")" + "informs me of a new member (Id: " + id1 + ") in heartbeat message. New member status: " + statusMap.get(Integer.parseInt(info1[2])) + ". Now added!");
                node.getMembers().put(id1, new ArrayList<String>() {{
                    add(info1[1]);
                    add(Long.toString(System.currentTimeMillis()));
                    add(info1[2]);
                }});
            }
        }
    }

    // Serialize membership list into a Java String
    // Each node is separated by ";"
    // Each field of node is separated by ","
    private String serialize(HashMap<String, ArrayList<String>> members) {
        String message = node.getId() + "," + node.getHbCounter() + "," + node.getStatus() + ";";

        for (String id : members.keySet()) {
            ArrayList<String> infoArray = members.get(id);
            message += id + "," + infoArray.get(0) + "," + infoArray.get(2) + ";"; // concatenate id, HbCounter, Timestamp, Status
        }
        return message.substring(0, message.length() - 1);
    }

    // Deserialize a Java String into an ArrayList<Node> that represents a node's membership list
    private HashMap<String, ArrayList<String>> deserialize(String membersString) {
        HashMap<String, ArrayList<String>> members = new HashMap<>();
        String[] stringList = membersString.split(";", 0);

        for (String stringNode : stringList) {
            String[] nodeInfo = stringNode.split(",", 0);

            members.put(nodeInfo[0], new ArrayList<String>() { // nodeInfo[0] : NodeId
                {
                    add(nodeInfo[1]); // HbCounter
                    add(Long.toString(System.currentTimeMillis())); // Timestamp
                    add(nodeInfo[2]); // Status
                }
            });
        }
        return members;
    }

    // Update membership list when a node has left the group
    private void updateNodeStatusInList(String nodeId, int status) {
        if (node.getMembers().containsKey(nodeId)) {
            node.getMembers().get(nodeId).set(2, Integer.toString(status)); // set the given node's status to "2"
        }
    }

    // Add a newly joined node to my membership list
    private void addNode(String nodeId) {
        node.addSingleMember(nodeId, new ArrayList<String>() {{
                    add("0");
                    add(Long.toString(System.currentTimeMillis()));
                    add("1");
                }}
        );
        logger.info("A new node (Id: " + nodeId + ") was added to my membership list");
    }

    // Send message to a given port at a given ip using a given socket
    private int sendMsg(DatagramSocket socket, String ip, int port, String message) {
        try {
            byte[] buffer = message.getBytes();

            cumulatedBytesOut += buffer.length; // Experiment: number of bytes to be sent out

            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, InetAddress.getByName(ip), port);
            socket.send(packet);
            return 0;
        } catch (SocketException e) {
            logger.info("sendMsg(): will not send message. Socket was closed.");
            return -1;
        } catch (IOException e) {
            logger.error("sendMsg(): exception occurs when sending packets.");
            e.printStackTrace();
            return -1;
        }
    }

    // Get a random sample of active nodes as the gossip list
    private Set<String> getGossipList(ArrayList<String> activeList) {
        int population = activeList.size();
        int sampleNum = (int) Math.ceil(population * SAMPLE_RATIO);  // Get the sample size using a sample ratio
        Random rand = new Random();
        Set<String> sample = new HashSet<>();
        while (sample.size() != sampleNum) {
            String id = activeList.get(rand.nextInt(population));
            if (!sample.contains(id)) {
                sample.add(id);
            }
        }
        return sample;
    }

    // Check whether the Introducer is normal or not
    private boolean isIntroducerNormal() {
        for (String id : node.getMembers().keySet()) {
            if (id.startsWith(INTRODUCER_PREFIX) && node.getMembers().get(id).get(2).equals("1")) {
                return true;
            }
        }
        return false;
    }

    // Check whether the given node (given node number) is normal or not, todo: refactor: merge this function with the function above.
    public boolean isNodeNormal(int nodeNumber) {
        for (String id : node.getMembers().keySet()) {
            if (id.startsWith(nodeNumber + "@") && node.getMembers().get(id).get(2).equals("1")) {
                return true;
            }
        }
        return false;
    }

    // Notify node's failure to re-replication thread
    private void notifyFailure(String failedNodeId) {
        int failedNodeNumber = getNodeNumberFromId(failedNodeId);
        sendMsg(socket, "localhost", SdfsServer.rereplicationPort, Integer.toString(failedNodeNumber));
        if (nodeNumber == 1) { //isMaster
            sendMsg(socket, "localhost", MapleJuice.reschedulePort, Integer.toString(failedNodeNumber));
        }
    }

    // Tell all normal nodes in my membership list to switch the protocol
    public void switchHelper() {
        ArrayList<String> normalIds = getNormalNodes(false);
        int protocol = this.protocol == 1 ? 2 : 1;
        for (String id : normalIds) {
            sendMsg(socket, getIpAddrFromId(id), receiverPortMap.get(getNodeNumberFromId(id)), "[SWITCH]" + protocol);
        }
        try {
            Thread.sleep(THREAD_TIME_OUT); // Wait for other nodes to switch the protocol. Switch my protocol afterwards.
            this.protocol = protocol;
            logger.info("switchHelper(): protocol changes to " + (protocol == 1 ? "All-to-All" : "Gossip"));
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("switchHelper(): InterruptionException is thrown.");
        }
    }

    // Heartbeat thread. It is used to send heartbeat to other nodes according to the selected protocol.
    private class HbThread extends Thread {
        @Override
        public void run() {

            if (protocol == 1) { // All-to-All
                while (runHeartBeat) {
                    try {
                        Thread.sleep(HB_PERIOD); // Send heartbeat every HB_PERIOD.

                        // We will always add introducer to gossip list if the following conditions are all met: (in order to support introducer rejoins the group)
                        // 1. Node itself is not an introducer.
                        // 2. The status of introducer in the list is either failed or left.
                        // 3. The node itself is currently in the group.
                        boolean includeIntroducer = !isIntroducer && node.getMembers().size() > 0 && !isIntroducerNormal();
                        ArrayList<String> idList = getNormalNodes(includeIntroducer);

                        // For experiment
                        Set<String> lostIds = new HashSet<>();
                        Random rand = new Random();
                        while (lostIds.size() != lostHbNum) { // Will not send hb to node in set: lostIds
                            String id = idList.get(rand.nextInt(idList.size()));
                            lostIds.add(id);
                        }

                        if (idList.size() > 0) { // Check condition: there exists normal nodes in the membership list.
                            node.incHb();
                            node.setCurTs(System.currentTimeMillis());
                            String members = serialize(node.getMembers()); // serialize membership list into a Java String

                            for (String curId : idList) { // All-to-all: send heartbeat to every normal node in the membership list.
                                if (!lostIds.contains(curId)) { // For experiment
                                    int num = getNodeNumberFromId(curId);
                                    sendMsg(socket, getIpAddrFromId(curId), receiverPortMap.get(num), "[HB]" + members); // send heartbeat
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        logger.error("HbThread::run(): InterruptedException thrown");
                        e.printStackTrace();
                    }
                }
            } else if (protocol == 2) { // Gossip style
                while (runHeartBeat) {
                    try {
                        Thread.sleep(HB_PERIOD); // Send heartbeat every HB_PERIOD.

                        // We will always add introducer to gossip list if the following conditions are all met: (in order to support introducer rejoins the group)
                        // Check the same condition as it in all2all protocol above.
                        boolean includeIntroducer = !isIntroducer && node.getMembers().size() > 0 && !isIntroducerNormal();
                        ArrayList<String> activeList = getNormalNodes(includeIntroducer);

                        int population = activeList.size();

                        if (population != 0) {
                            Set<String> sample = getGossipList(activeList); // retrieve a **random** sample from normal nodes
                            node.incHb();
                            node.setCurTs(System.currentTimeMillis());
                            String members = serialize(node.getMembers());

                            // For experiment
                            ArrayList<String> sampleArray = new ArrayList<>(sample);
                            Set<String> lostIds = new HashSet<>();
                            Random rand = new Random();
                            while (lostIds.size() != lostHbNum) { // Will not send hb to node in set: lostIds
                                String id = sampleArray.get(rand.nextInt(sampleArray.size()));
                                lostIds.add(id);
                            }

                            for (String id : sample) {
                                if (!lostIds.contains(id)) {
                                    int num = getNodeNumberFromId(id);
                                    //                              logger.debug(String.format("Node id: %1$s send gossip to Node id: %2$s", node.getId(), id));
                                    sendMsg(socket, getIpAddrFromId(id), receiverPortMap.get(num), "[HB]" + members); // send heartbeat
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    // Receiver thread, used to process message received from other nodes.
    private class ReceiverThread extends Thread {
        @Override
        public void run() {
            try {
                receiverSocket = new DatagramSocket(receiverPort);

                while (runReceiver) {
//                    logger.debug("In Receiver Thread: waiting for new message");

                    byte[] buffer = new byte[2048];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                    receiverSocket.receive(packet);

                    cumulatedBytesIn += packet.getLength(); // For experiment, calculate the number of bytes received

                    if (node.getStatus() == 2) { // Node left => do nothing
                        continue;
                    }
//                    logger.debug("receiver receive msg");
                    InetAddress address = packet.getAddress();
                    int port = packet.getPort();
                    packet = new DatagramPacket(buffer, buffer.length, address, port);

                    String message = new String(packet.getData(), 0, packet.getLength()).trim();

                    if (message.startsWith("[HB]")) { // Receive other node's heartbeat => update membership list
                        String[] members = message.substring(4).split(";", 0);
                        mergeMembers(members, protocol);
                    } else if (message.startsWith("[LEAVE]")) { // Receives a leave request
                        String leaveNode = message.substring(7);
                        updateNodeStatusInList(leaveNode, 2);
                        notifyFailure(leaveNode);
                        logger.info("A node (ID: " + leaveNode + ") left the group.");
                    } else if (message.startsWith("[SWITCH]")) { // Receive a protocol switching request
                        protocol = Integer.parseInt(message.substring(8));
                        logger.info("Switched to protocol: " + (protocol == 1 ? "All-to-All" : "Gossip"));
                    } else if (message.startsWith("[LOST]")) { // For experiment
                        lostHbNum = Integer.parseInt(message.substring(6));
                        logger.info("Current node lost " + lostHbNum + " HB in each HB period");
                    }
                }
            } catch (SocketException e) {
                logger.info("Closing receiver socket");
            } catch (IOException e) {
                logger.error("ReceiverThread::run(): exception thrown");
                e.printStackTrace();
            } finally { // finally close the socket
                if (receiverSocket != null) {
                    receiverSocket.close();
                }
                logger.info("Receiver socket closed");
            }

        }
    }

    // Detector thread, used to detect potential node failure.
    private class DetectorThread extends Thread {
        @Override
        public void run() {
            while (runDetector) {
                try {
                    Long T_FAIL = protocol == 1 ? T_FAIL_ALL2ALL : T_FAIL_GOSSIP; // 1 stands for ALL-TO-ALL, 2 stands for Gossip
                    Thread.sleep(T_FAIL);
                    HashMap<String, ArrayList<String>> members = node.getMembers();
                    for (String curId : members.keySet()) {
                        if (members.get(curId).get(2).equals("1")) { //only check normal nodes
                            Long latestTs = Long.parseLong(members.get(curId).get(1));
                            if (System.currentTimeMillis() - latestTs > T_FAIL) {   // Mark node as failed if its timestamp difference is greater than T_FAIL
                                members.get(curId).set(2, "0");
                                notifyFailure(curId);
                                logger.info("DetectorThread: A failed Node with Id = " + curId + " is detected.");
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    logger.error("DetectorThread::run(): InterruptedException is thrown");
                    e.printStackTrace();
                }
            }
        }
    }

    // Introducer Thread. Run on introducer vm and serve join request from new nodes
    private class IntroducerThread extends Thread {
        @Override
        public void run() {
            try {
                introducerSocket = new DatagramSocket(INTRODUCER_PORT);

                while (runIntroducer) {
                    byte[] buffer = new byte[50];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    introducerSocket.receive(packet);

                    if (node.getStatus() == 2) { // Node left => do nothing
                        continue;
                    }

                    InetAddress address = packet.getAddress();
                    int port = packet.getPort();
                    packet = new DatagramPacket(buffer, buffer.length, address, port);

                    String newNodeId = new String(packet.getData(), 0, packet.getLength()).trim(); // message received = server id (format: nodeNumber@ipAddr@timestamp)
                    String newNodeIp = getIpAddrFromId(newNodeId);

                    logger.info("IntroducerThread::run(): Introducer receive a join request from Node: " + newNodeId);

                    HashMap<String, ArrayList<String>> members = node.getMembers();
                    if (!members.containsKey(newNodeId)) {
                        addNode(newNodeId);
                    }

                    String message = serialize(node.getMembers());
                    int response = sendMsg(socket, newNodeIp, senderPortMap.get(getNodeNumberFromId(newNodeId)), "[LIST]" + message); // message sent to senderSocket

                    if (response != 0) {
                        logger.warn("Introducer fails to send membership list to the new node");
                    }

                }
            } catch (SocketException e) {
                logger.info("Closing introducer socket...");
            } catch (IOException e) {
                logger.error("IntroducerThread::run(): IOException");
                e.printStackTrace();
            } finally {
                if (introducerSocket != null) { // close introducer socket at last if it is not null
                    introducerSocket.close();
                }
                logger.info("Introducer socket closed.");
            }
        }

    }
}
