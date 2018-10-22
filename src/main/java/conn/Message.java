package conn;

import java.io.Serializable;
import java.util.Map;

public class Message implements Serializable {

  public enum MessageType {
    INIT, HELLO, TC, DATA
  }

  MessageType type;
  private int senderId;
  Serializable dataload;

  public Message(MessageType type, int senderId, Serializable dataload) {
    this.type = type;
    this.senderId = senderId;
    this.dataload = dataload;
  }

  public int getSenderId() {
    return senderId;
  }

}
