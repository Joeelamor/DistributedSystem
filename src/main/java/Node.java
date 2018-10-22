import conn.Conn;
import conn.OLSRConn;
import org.apache.commons.lang3.tuple.Pair;
import parser.Parser;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

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
    this.conn = new OLSRConn(this.nodeId, this.port);
    this.conn.connect(this.connectionList);
  }

  public void start() {
    // TODO: capable of many things.
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
