package edu.cs425.mp3;

public class MapleJob extends Job {

    private String sdfsSrcDir; // Maple Job input file source SDFS directory

    // Constructor
    public MapleJob(int taskNum, String executableFile, String sifPrefix, int initializerId, String sdfsSrcDir) {
        super(taskNum, executableFile, sifPrefix, initializerId);
        this.sdfsSrcDir = sdfsSrcDir;
    }

    // Getter
    public String getSdfsSrcDir() {
        return sdfsSrcDir;
    }

    // Setter
    public void setSdfsSrcDir(String sdfsSrcDir) {
        this.sdfsSrcDir = sdfsSrcDir;
    }

    // Serialization
    @Override
    public String serializeJob() {
        return getExecutableFile() + "@" + getTaskNum() + "@" + getSifPrefix() + "@" + getInitializerId() + "@" + sdfsSrcDir;
    }

    // Deserialization
    public static MapleJob deserializeJobStr(String mapleJobStr) {
        String[] jobInfo = mapleJobStr.split("@");
        String executableFile = jobInfo[0];
        int taskNum = Integer.parseInt(jobInfo[1]);
        String sifPrefix = jobInfo[2];
        int initializerId = Integer.parseInt(jobInfo[3]);
        String sdfsSrcDir = jobInfo[4];

        return new MapleJob(taskNum, executableFile, sifPrefix, initializerId, sdfsSrcDir);
    }
}
