package conn;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public class LSRConn extends SimpleConn {

  /**
   * Each node should periodically floods {@code HELLO} message for link
   * sensing. The node’s MPR set is included in the {@code HELLO} message.
   * Nodes with a non-empty {@code MS} periodically flood their {@code MS}
   * via a {@code TC} message.
   *
   * The {@code LSRUpdater} run as a thread periodically send out these messages.
   */
  private class LSRUpdater implements Runnable {
    @Override
    public void run() {

    }
  }

  private Set<Integer> OneHopNeighbor;
  private Map<Integer, Set<Integer>> TwoHopNeighbor;

  private Set<Integer> MultiPointRelays;
  private long MultiPointRelaysSeq;

  private Set<Integer> MPRSelectors;
  private long MPRSelectorsSeq;

  public LSRConn(int nodeId, int port) throws IOException {
    super(nodeId, port);
    new Thread(new LSRUpdater()).start();
  }

  @Override
  public void send(int id, Serializable message) {
    super.send(id, message);
  }

  @Override
  public void broadcast(Serializable message) {
    super.broadcast(message);
  }

  @Override
  public Serializable getMessage() {
    return super.getMessage();
  }
}
