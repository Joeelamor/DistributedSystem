package parser;

import java.io.*;
import java.util.*;
import java.net.*;

public class Parser {

    public static class HostInfo {
        public int nodeId;
        public String hostname;
        public int port;
        public Map<Integer, List<String>> neighbors;

        public HostInfo(int nodeId, String hostname, int port, Map<Integer, List<String>> neighbors) {
            this.nodeId = nodeId;
            this.hostname = hostname;
            this.port = port;
            this.neighbors = neighbors;
        }

        @Override
        public String toString() {
            return "HostInfo{" +
                    "nodeId=" + nodeId +
                    ", hostname='" + hostname + '\'' +
                    ", port=" + port +
                    ", neighbors=" + neighbors +
                    '}';
        }
    }

    private int totalNumber;

    private List<HostInfo> hostInfos;

    public Parser() {
        this.totalNumber = 0;
        this.hostInfos = new ArrayList<>();
    }

    public int getTotalNumber() {
        return totalNumber;
    }

    public List<HostInfo> getHostInfos() {
        return hostInfos;
    }

    public void parseFile(String path, String Hostname) throws FileNotFoundException {

        HashMap<Integer, List<Integer>> neighborList = new HashMap<>();
        HashMap<Integer, List<String>> serverInfo = new HashMap<>();

        File file = new File(path);
        Scanner sc = new Scanner(file);

        while (sc.hasNextLine()) {
            String line = sc.nextLine().trim();
            if (line.length() == 0 || !Character.isDigit(line.charAt(0))) {
                continue;
            }
            this.totalNumber = Integer.parseInt(line);
            break;
        }

        int id = 1;
        List<Integer> ids = new ArrayList<>();
        while (sc.hasNextLine()) {
            String line = sc.nextLine().trim();
            if (line.length() == 0 || !Character.isLetter(line.charAt(0))) {
                continue;
            }

            String[] strs = line.split("\\s+");
            serverInfo.put(id, Arrays.asList(strs[0], strs[1]));

            String host = strs[0] + ".utdallas.edu";


            if (!neighborList.containsKey(id)) {
                neighborList.put(id, new ArrayList<>());
            }
            for (int i = 2; i < strs.length; i++)
                neighborList.get(id).add(Integer.parseInt(strs[i]));

            if (host.equals(Hostname))
                ids.add(id);

            id++;
        }

        for (int hostId : ids) {
            String hostname = serverInfo.get(hostId).get(0);
            int port = Integer.parseInt(serverInfo.get(hostId).get(1));
            Map<Integer, List<String>> neighborInfo = new HashMap<>();
            List<Integer> neighbors = neighborList.get(hostId);
            for (int neighborId : neighbors) {
                neighborInfo.put(neighborId, serverInfo.get(neighborId));
            }
            this.hostInfos.add(new HostInfo(hostId, hostname, port, neighborInfo));
        }
    }

    @Override
    public String toString() {
        return "Parser{" +
                ", totalNumber=" + totalNumber +
                ", hostInfos=" + hostInfos +
                '}';
    }

    public static void main(String[] args) throws UnknownHostException {
        Parser test = new Parser();
        String Hostname = InetAddress.getLocalHost().getHostName();
        try {
            test.parseFile("/Users/lizhong/Github/CS6378/Project2/config.txt", "dc33.utdallas.edu");
            System.out.println(test.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}