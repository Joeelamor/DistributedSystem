package conn;

import java.io.Serializable;

public interface Conn {
  void send(int id, Serializable message);
  void broadcast(Serializable message);
}
