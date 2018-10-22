package conn;

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public interface Conn {
  void connect(Map<Integer, Pair<String, Integer>> connectionList) throws IOException;

  void send(int id, Serializable message);

  void broadcast(Serializable message);

  Serializable getMessage();
}
