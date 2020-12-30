package edu.cs425.mp3;

import java.util.ArrayList;
import java.util.HashMap;

class Node {
    private String ipAddr; // Node ip address

    private long curTs; // Node's current timestamp

    private long createdTs; // timestamp when node was created

    private String id; // Node id

    private int status = 1;  // Node status: 0 for failed, 1 for normal, 2 for left

    private int hbCounter = 0; // Node's heartbeat counter

    private int nodeNumber; // Node number (vm, range: [1, 10])

    private HashMap<String, ArrayList<String>> members = new HashMap<>(); // <K, V>, K is Node id, V is ["HbCounter", "Timestamp", "Status"]

    // Default constructor
    public Node() { }

    // Constructor of the Node class
    public Node(int nodeNumber, String ipAddr, long ts) {
        this.curTs = ts;
        this.createdTs = ts;
        this.ipAddr = ipAddr;
        this.nodeNumber = nodeNumber;
        this.id = nodeNumber + "@" + ipAddr + "@" + ts;
    }

    // Getter
    public HashMap<String, ArrayList<String>> getMembers() {
        return members;
    }

    // Setter
    public void setMembers(HashMap<String, ArrayList<String>> members) {
        this.members = members;
    }

    // Add single member to membership list
    public void addSingleMember(String id, ArrayList<String> infoArr) {
        members.put(id, infoArr);
    }

    // Getter
    public int getNodeNumber() {
        return nodeNumber;
    }

    // Increment heartbeat counter
    public void incHb() {
        hbCounter++;
    }

    // Getter
    public int getHbCounter() {
        return hbCounter;
    }

    // Setter
    public void setHbCounter(int hbCounter) {
        this.hbCounter = hbCounter;
    }

    // Get ip address
    public String getIpAddr() {
        return ipAddr;
    }

    // Set ip address
    public void setIpAddr(String ipAddr) {
        this.ipAddr = ipAddr;
    }

    // Get node created time
    public long getCreatedTs() {
        return createdTs;
    }

    // Get the timestamp of node (the timestamp when node was created)
    public long getCurTs() {
        return curTs;
    }

    // Set the timestamp of node
    public void setCurTs(long curTs) {
        this.curTs = curTs;
    }

    // Get the id of the node
    public String getId() {
        return id;
    }

    // Set the id of the node
    public void setId(String id) {
        this.id = id;
    }

    // Get the status of the node
    public int getStatus() {
        return status;
    }

    // Set the status of the node
    public void setStatus(int status) {
        this.status = status;
    }
}
