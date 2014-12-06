package kvstore;

import static kvstore.KVConstants.DEL_REQ;
import static kvstore.KVConstants.ERROR_COULD_NOT_CONNECT;
import static kvstore.KVConstants.ERROR_COULD_NOT_CREATE_SOCKET;
import static kvstore.KVConstants.ERROR_NO_SUCH_KEY;
import static kvstore.KVConstants.ERROR_INVALID_KEY;
import static kvstore.KVConstants.ERROR_INVALID_VALUE;
import static kvstore.KVConstants.GET_REQ;
import static kvstore.KVConstants.PUT_REQ;
import static kvstore.KVConstants.RESP;
import static kvstore.KVConstants.SUCCESS;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Client API used to issue requests to key-value server.
 */
public class KVClient implements KeyValueInterface {

    public String server;
    public int port;

    /**
     * Constructs a KVClient connected to a server.
     *
     * @param server is the DNS reference to the server
     * @param port is the port on which the server is listening
     */
    public KVClient(String server, int port) {
        this.server = server;
        this.port = port;
    }

    /**
     * Creates a socket connected to the server to make a request.
     *
     * @return Socket connected to server
     * @throws KVException if unable to create or connect socket
     */
    public Socket connectHost() throws KVException
    {
        try {
            return new Socket(this.server, this.port);
        } catch (NullPointerException | SecurityException | IllegalArgumentException e ) {
            throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
        } catch (IOException e) {
            throw new KVException(ERROR_COULD_NOT_CONNECT);
        }
    }

    /**
     * Closes a socket.
     * Best effort, ignores error since the response has already been received.
     *
     * @param  sock Socket to be closed
     */
    public void closeHost(Socket sock) {
	try {
	    sock.close();
	} catch (IOException e) {}
    }


    private boolean is_valid(String val) {
      if ((val == null) || (val.isEmpty())) {
        return false;
      } else {
        return true;
      }
    }
    /**
     * Issues a PUT request to the server.
     *
     * @param  key String to put in server as key
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void put(String key, String value) throws KVException {
      if (this.is_valid(key) == false) {
        throw new KVException(ERROR_INVALID_KEY);
      }
      if (this.is_valid(value) == false) {
        throw new KVException(ERROR_INVALID_VALUE);
      }

	    KVMessage putRequest = new KVMessage(PUT_REQ);
	    putRequest.setKey(key);
	    putRequest.setValue(value);
	    Socket sock = connectHost();
	    putRequest.sendMessage(sock);
	    KVMessage receive = new KVMessage(sock);
        closeHost(sock)
	    if (!receive.getMessage().equals(SUCCESS)) {
	       throw new KVException(receive.getMessage());
	    }
    }

    /**
     * Issues a GET request to the server.
     *
     * @param  key String to get value for in server
     * @return String value associated with key
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public String get(String key) throws KVException {
      if (this.is_valid(key) == false) {
        throw new KVException(ERROR_INVALID_KEY);
      }

	    KVMessage getRequest = new KVMessage(GET_REQ);
	    getRequest.setKey(key);
	    Socket sock = connectHost();
	    getRequest.sendMessage(sock);
	    KVMessage response = new KVMessage(sock);
        closeHost(sock)
	    if (response.getKey() == null) {
	       throw new KVException(ERROR_NO_SUCH_KEY);
	    }
	    return response.getValue();
    }

    /**
     * Issues a DEL request to the server.
     *
     * @param  key String to delete value for in server
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void del(String key) throws KVException {
      if (this.is_valid(key) == false) {
        throw new KVException(ERROR_INVALID_KEY);
      }

	    KVMessage delRequest = new KVMessage(DEL_REQ);
	    delRequest.setKey(key);
	    Socket sock = connectHost();
	    delRequest.sendMessage(sock);
	    KVMessage response = new KVMessage(sock);
        closeHost(sock)
	    if (!response.getMessage().equals(SUCCESS)) {
	       throw new KVException(ERROR_NO_SUCH_KEY);
	    }
    }


}
