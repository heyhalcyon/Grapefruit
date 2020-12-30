package edu.cs425.mp3;

import java.util.HashSet;

/**
 * FileStatus enumeration (3 status): NOOP (no operation), READING (file being read), Writing (file being written)
 */
enum FileStatus {
    NOOP,
    READING,
    WRITING
}

/**
 * SdfsFile class: model file stored in simple distribute file system (SDFS)
 */
class SdfsFile {

    private String sdfsFileName; // file name in SDFS system

    private String localFilePath; // local file path of SDFS file on the node

    private HashSet<Integer> replicaSet; // Set of replicas where the SDFS file is store

    private FileStatus status = FileStatus.NOOP; // Status of this SDFS file: 0 for no-op (no operation, default value), 1 for reading, 2 for writing

    private String directoryPath = "/";

    private long timestamp = -1; // Last modified timestamp of this SDFS file

    // Constructor with 3 arguments: sdfs file name, local file path, replica set
    public SdfsFile(String sdfsFileName, String localFilePath, HashSet<Integer> replicaSet, String directoryPath) {
        this.sdfsFileName = sdfsFileName;
        this.localFilePath = localFilePath;
        this.replicaSet = replicaSet;
        this.directoryPath = directoryPath;
    }

    // Constructor with 4 arguments: sdfs file name, local file path, replicas set, timestamp
    public SdfsFile(String sdfsFileName, String localFilePath, HashSet<Integer> replicaSet, String directoryPath, long timestamp) {
        this(sdfsFileName, localFilePath, replicaSet, directoryPath);
        this.timestamp = timestamp;
    }

    // Constructor with 5 arguments: sdfs file name, local file path, replicas set, timestamp, status
    public SdfsFile(String sdfsFileName, String localFilePath, HashSet<Integer> replicaSet, String directoryPath, long timestamp, FileStatus status) {
        this(sdfsFileName, localFilePath, replicaSet, directoryPath, timestamp);
        this.status = status;
    }

    // Getter: return the file name in SDFS system
    public String getSdfsFileName() {
        return sdfsFileName;
    }

    // Setter: set SDFS file name for the SDFS file
    public void setSdfsFileName(String sdfsFileName) {
        this.sdfsFileName = sdfsFileName;
    }

    // Getter: return SDFS file's local path on current node
    public String getLocalFilePath() {
        return localFilePath;
    }

    // Setter: set SDFS file's local path on current node
    public void setLocalFilePath(String localFilePath) {
        this.localFilePath = localFilePath;
    }

    // Getter: return SDFS file's replica set
    public HashSet<Integer> getReplicaSet() {
        return replicaSet;
    }

    // Setter: set SDFS file's replica set
    public void setReplicaSet(HashSet<Integer> replicaSet) {
        this.replicaSet = replicaSet;
    }

    public String getDirectoryPath() {
        return directoryPath;
    }

    public void setDirectoryPath(String directoryPath) {
        this.directoryPath = directoryPath;
    }

    // Getter: return the last modified timestamp of this SDFS file
    public long getTimestamp() {
        return timestamp;
    }

    // Setter: set the last modified timestamp of this SDFS file
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    // Getter: return the current status of this SDFS file
    public FileStatus getStatus() {
        return status;
    }

    // Setter: set the status of this SDFS file
    public void setStatus(FileStatus status) {
        this.status = status;
    }
}
