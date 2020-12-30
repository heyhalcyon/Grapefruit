package edu.cs425.mp3;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * A class of utility functions, such as send message, request file, serialization, deserialization, status checking, etc.
 */
public class Utility {

    public Utility() {
    }

    // Hash file name => Node number in the range
    public static int hashFile(String fileName, int totalNormalNodeNumber) {
        // Use Java object's built-in hashcode method, totalNormalNodeNumber: the total number of normal nodes in the group
        return Math.abs(fileName.hashCode()) % totalNormalNodeNumber + 1;
    }

    // Send message to a given port at a given ip using a given socket
    public static int sendMsg(DatagramSocket socket, String ip, int port, String message) {
        try {
            byte[] buffer = message.getBytes();
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, InetAddress.getByName(ip), port);
            socket.send(packet);
            return 0;
        } catch (SocketException e) {
            SdfsServer.logger.info("sendMsg(): will not send message. Socket was closed.");
            return -1;
        } catch (IOException e) {
            SdfsServer.logger.error("sendMsg(): exception occurs when sending packets.");
            e.printStackTrace();
            return -1;
        }
    }

    // Send SDFS file request to given node. After getting the file, write the file locally with the given local file name.
    public static HashMap<String, ArrayList<String>> fileRequest(String ip, int port, String requestFileName, String outputFileName, boolean isMapleJuiceTransfer) {
        Socket clientSocket;
        PrintWriter out;
        BufferedReader in;
        HashMap<String, ArrayList<String>> kVs = new HashMap<>();
        try {
            clientSocket = new Socket(ip, port);
            out = new PrintWriter(clientSocket.getOutputStream(), true); // Autoflush
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            SdfsServer.logger.debug("fileRequest(): sending file request to server...");
            out.println("[FILE_REQ]" + requestFileName);

            // Create a new file locally and write server response to the file
            File file = new File(outputFileName);
            PrintWriter filePrinter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file)), true);

            if (isMapleJuiceTransfer) { // local file transfer upon maple request
                String line;
                while ((line = in.readLine()) != null) {
                    line = line.trim();
                    if (line.length() > 0) {
                        String[] kv = line.split(",");
                        String key = kv[0], value = kv[1];

                        if (kVs.containsKey(key)) {
                            kVs.get(key).add(value);
                        } else {
                            ArrayList<String> valueList = new ArrayList<>();
                            valueList.add(value);
                            kVs.put(key, valueList);
                        }
                    }
                }
            } else { // for SDFS function's file transfer
                int length = 2048;
                char[] charArr = new char[length];
                int charRead = in.read(charArr, 0, length);

                SdfsServer.logger.info("Writing to file: " + outputFileName);
                while (charRead != -1) { // Write to file locally
                    filePrinter.write(charArr, 0, charRead);
                    charRead = in.read(charArr, 0, length);
                }
            }

            // Close input & output stream and socket.
            filePrinter.close();
            out.close();
            in.close();
            clientSocket.close();
        } catch (IOException e) { // Exception handler
            SdfsServer.logger.error("fileRequest(): error I/O occurs when creating Socket");
            e.printStackTrace();
            return null; // On failure, return kVs
        }
        return kVs; // On success, return kVs
    }

    // Check whether two operations (on the same file) can be done simultaneously
    public static boolean isOperationAllowed(FileStatus currentOperation, FileStatus operation) {
        if (currentOperation == FileStatus.NOOP) { // Currently, the file is not operated by other nodes. => can perform any operation on this file.
            return true;
        } else if (currentOperation == FileStatus.WRITING) { // File is being written. Other nodes' writing & reading operations are not allowed.
            return false;
        } else if (operation == FileStatus.READING) { // File is being read. Only simultaneous reading is allowed. Writing is rejected.
            return true;
        }
        return false;
    }

    // Serialize Java Set object to a Java String (using predefined format)
    public static String serializeSet(HashSet<Integer> aSet) {
        String res = "";
        for (Integer i : aSet) {
            res += i + ",";
        }
        SdfsServer.logger.debug("Serialized to: " + res.substring(0, res.length() - 1));
        return res.substring(0, res.length() - 1);
    }

    // Deserialized Java String into Java HashSet object (using predefined format)
    public static HashSet<Integer> deserializeSet(String setStr) {
        HashSet<Integer> res = new HashSet<>();
        if(setStr.length() == 0) return res;
        String[] numArr = setStr.split(",", 0);
        for (String s : numArr) {
            if(s.length() > 0) {
                res.add(Integer.parseInt(s));
            }
        }
        SdfsServer.logger.debug("Deserialized to: " + res.toString());
        return res;
    }

    // Serialize SdfsFile object into Java String (using predefined format)
    public static String serializeSdfsFile(SdfsFile sdfsFile) {
        // Format: <SDFS file name>@<local file path>@<replica set>@status@timestamp
        return sdfsFile.getSdfsFileName() + "@" + "" + "@" + serializeSet(sdfsFile.getReplicaSet()) + "@" + sdfsFile.getStatus() + "@" + sdfsFile.getTimestamp() + "@" + sdfsFile.getDirectoryPath();
    }

    // Deserialize Java String into SdfsFile object (using predefined format)
    public static SdfsFile deserializeSdfsFileStr(String sdfsFileStr) {
        String[] info = sdfsFileStr.split("@", 0);
        String sdfsFileName = info[0];
        String localFilePath = info[1];
        HashSet<Integer> replicaSet = deserializeSet(info[2]);
        FileStatus status = FileStatus.valueOf(info[3]);
        long timestamp = Long.parseLong(info[4]);
        String directoryPath = info[5];

        return new SdfsFile(sdfsFileName, localFilePath, replicaSet, directoryPath, timestamp, status);
    }

    // Initiate port map (map node number to the port where processes run)
    public static void initiatePortMap(HashMap<Integer, Integer> portMap, int nodeLimit, int portStart) {
        for (int i = 1; i <= nodeLimit; i++) {
            portMap.put(i, portStart + i - 1);
        }
    }

    // Extract file information (e.g. file path, file name) given a complete file name string
    public static String[] extractFileInfo(String fileName) {
        String[] infoArray = fileName.replaceFirst("/+", "").replaceAll("/$", "").split("/");
        String directoryPath = "/";

        for (int i = 0; i < infoArray.length - 1; i++) {
            directoryPath += infoArray[i] + "/";
        }

        return new String[]{directoryPath, infoArray[infoArray.length - 1]};
    }

    // Run executable file in Java program. Support configuration.
    public static void runCommand(String[] exeCommand, PrintWriter outputFileWriter) {
        try {
            Process proc = Runtime.getRuntime().exec(exeCommand);
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String s;
            while ((s = stdInput.readLine()) != null) {
                if (s.length() > 0) {
                    outputFileWriter.println(s);
                }
            }
            stdInput.close();
        } catch (IOException e) {
            MapleJuice.logger.error("runCommand(): IOException occurs.");
            e.printStackTrace();
        }
    }
}
