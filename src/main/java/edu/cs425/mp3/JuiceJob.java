package edu.cs425.mp3;

enum ShuffleOption { // Available shuffle/partition mechanism: hash / range
    HASH,
    RANGE
}


public class JuiceJob extends Job {

    private String sdfsDestFilename; // Juice job output file name
    private boolean deleteInput; // false if not delete input file, true if delete input file after juice job completes
    private ShuffleOption shuffleOption; // shuffle option: hash / partition

    // Constructor
    public JuiceJob(int taskNum, String executableFile, String sifPrefix, int initializerId, String sdfsDestFilename, boolean deleteInput, ShuffleOption shuffleOption) {
        super(taskNum, executableFile, sifPrefix, initializerId);
        this.sdfsDestFilename = sdfsDestFilename;
        this.deleteInput = deleteInput;
        this.shuffleOption = shuffleOption;
    }

    // Getter
    public String getSdfsDestFilename() {
        return sdfsDestFilename;
    }

    // Setter
    public void setSdfsDestFilename(String sdfsDestFilename) {
        this.sdfsDestFilename = sdfsDestFilename;
    }

    // Getter
    public boolean getDeleteInput() {
        return deleteInput;
    }

    // Setter
    public void setDeleteInput(boolean deleteInput) {
        this.deleteInput = deleteInput;
    }

    // Getter
    public ShuffleOption getShuffleOption() {
        return shuffleOption;
    }

    // Setter
    public void setShuffleOption(ShuffleOption shuffleOption) {
        this.shuffleOption = shuffleOption;
    }

    // Serialization
    @Override
    public String serializeJob() {
        return getExecutableFile() + "@" + getTaskNum() + "@" + getSifPrefix() + "@" + getInitializerId() +
                "@" + sdfsDestFilename + "@" + (deleteInput ? 1 : 0) + "@" + (shuffleOption == ShuffleOption.HASH ? 1 : 2);
    }

    // Deserialization
    public static JuiceJob deserializeJobStr(String juiceJobStr) {
        String[] jobInfo = juiceJobStr.split("@");
        String executableFile = jobInfo[0];
        int taskNum = Integer.parseInt(jobInfo[1]);
        String sifPrefix = jobInfo[2];
        int initializerId = Integer.parseInt(jobInfo[3]);
        String sdfsDestDir = jobInfo[4];
        boolean deleteInput = Integer.parseInt(jobInfo[5]) == 1 ? true : false;
        ShuffleOption shuffleOption = Integer.parseInt(jobInfo[6]) == 1 ? ShuffleOption.HASH : ShuffleOption.RANGE;

        return new JuiceJob(taskNum, executableFile, sifPrefix, initializerId, sdfsDestDir, deleteInput, shuffleOption);
    }
}
