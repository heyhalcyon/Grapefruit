package edu.cs425.mp3;

public abstract class Job {

    private int taskNum; // Task ID
    private String executableFile; // Task Executable file
    private String sifPrefix; // SDFS Intermediate File Prefix
    private int initializerId; // ID of node that initiates Maple or Juice job

    // Constructor
    public Job(int taskNum, String executableFile, String sifPrefix, int initializerId) {
        this.taskNum = taskNum;
        this.executableFile = executableFile;
        this.sifPrefix = sifPrefix;
        this.initializerId = initializerId;
    }

    // Getter
    public int getTaskNum() {
        return taskNum;
    }

    // Setter
    public void setTaskNum(int taskNum) {
        this.taskNum = taskNum;
    }

    // Getter
    public String getExecutableFile() {
        return executableFile;
    }

    // Setter
    public void setExecutableFile(String executableFile) {
        this.executableFile = executableFile;
    }

    // Getter
    public String getSifPrefix() {
        return sifPrefix;
    }

    // Setter
    public void setSifPrefix(String sifPrefix) {
        this.sifPrefix = sifPrefix;
    }

    // Getter
    public int getInitializerId() {
        return initializerId;
    }

    // Setter
    public void setInitializerId(int initializerId) {
        this.initializerId = initializerId;
    }

    // Serialization: serialize Job object to Java String
    public abstract String serializeJob();

}
