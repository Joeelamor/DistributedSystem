import conn.Conn;
import conn.SimpleConn;
import conn.LSRConn;
import org.apache.commons.lang3.tuple.Pair;
import parser.Parser;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

public class Node {
  private Map<Integer, Pair<String, Integer>> connectionList;
  private int nodeId;
  private int port;
  private int totalNumber;
  private Conn conn;

  public Node(Map<Integer, Pair<String, Integer>> connectionList, int nodeId, int port, int totalNumber) {
    this.connectionList = connectionList;
    this.nodeId = nodeId;
    this.port = port;
    this.totalNumber = totalNumber;
  }

  public void init() throws IOException {
    this.conn = new LSRConn(this.nodeId, this.port);
    this.conn.connect(this.connectionList);
  }

  public void start() {

    long startTime = new Date().getTime();
    long curTime = 0;
    long timeout = totalNumber * 10 * 1000;
    while (curTime - startTime < timeout) {
      Serializable message = conn.getMessage();
      curTime = new Date().getTime();
    }
    TreeMap<Integer, List<Integer>> output = new TreeMap<>();

    System.out.println("Node " + nodeId + "'s khop: " +
      output.entrySet()
        .stream()
        .map(entry -> entry.getKey() + ":" + entry.getValue())
        .collect(Collectors.joining(", \n\t", "{\n\t", "\n}")));
  }

  @Override
  public String toString() {
    return "Node[" + nodeId + ":" + port + ']';
  }

  public static void main(String[] args) throws FileNotFoundException {
    Parser parser = new Parser();
    String hostName = "";
    try {
      hostName = InetAddress.getLocalHost().getHostName();
      System.out.println(hostName);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    parser.parseFile(args[0], hostName);
    int totalNumber = parser.getTotalNumber();
    List<Parser.HostInfo> hostInfos = parser.getHostInfos();
    for (Parser.HostInfo hostInfo : hostInfos) {
      try {
        Node node = new Node(
          hostInfo.neighbors,
          hostInfo.nodeId,
          hostInfo.host.getRight(),
          totalNumber
        );
        node.init();
        node.start();
        return;
      } catch (IOException e) {
        System.out.printf("%s already started, try next port.\n", hostInfo);
      }
    }
  }
}
