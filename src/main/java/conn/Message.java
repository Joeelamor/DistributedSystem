package conn;

import java.io.Serializable;
import java.util.Map;

public class Message implements Serializable {

  public enum MessageType {
    HELLO, TC, DATA
  }


  MessageType type;
  private int senderId;
  public Map<Integer, Integer> neighbors;

  public Message(int senderId, Map<Integer, Integer> neighbors) {
    this.senderId = senderId;
    this.neighbors = neighbors;
  }

  public int getSenderId() {
    return senderId;
  }

  public Map<Integer, Integer> getNeighbors() {
    return neighbors;
  }

  @Override
  public String toString() {
    return "conn.Message{" +
      "senderId=" + senderId +
      '}';
  }
}
