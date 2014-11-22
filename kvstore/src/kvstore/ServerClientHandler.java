package kvstore;

import static kvstore.KVConstants.DEL_REQ;
import static kvstore.KVConstants.GET_REQ;
import static kvstore.KVConstants.PUT_REQ;
import static kvstore.KVConstants.RESP;
import static kvstore.KVConstants.SUCCESS;

import java.io.IOException;
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * Uses a thread pool to ensure that none of its methods are blocking.
 */
public class ServerClientHandler implements NetworkHandler {

    public KVServer kvServer;
    public ThreadPool threadPool;

    /**
     * Constructs a ServerClientHandler with ThreadPool of a single thread.
     *
     * @param kvServer KVServer to carry out requests
     */
    public ServerClientHandler(KVServer kvServer) {
        this(kvServer, 1);
    }

    /**
     * Constructs a ServerClientHandler with ThreadPool of thread equal to
     * the number passed in as connections.
     *
     * @param kvServer KVServer to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public ServerClientHandler(KVServer kvServer, int connections) {
        this.kvServer = kvServer;
        threadPool = new ThreadPool(connections);
    }

    /**
     * Creates a job to service the request for a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param client Socket connected to the client with the request
     */
    @Override
    public void handle(Socket client) {
        KVMessage message;
        try {
            message = new KVMessage(client);
        } catch (KVException e) {
            message = new KVMessage(RESP, e.getKVMessage().getMessage());
            try {
                message.sendMessage(client);
	    } catch (KVException f) {}
	    if (!client.isClosed()) {
		try {
		    client.close();
		} catch (IOException g) {}
	    }
	    return;
	}
        String messType = message.getMsgType();
        String key = message.getKey();
	String val = message.getValue();
	threadPool.addJob(new RequestThread(key, val, messType, client, kvServer));
	
    }
    
    private static class RequestThread extends Thread {
        private String key;
        private String value;
        private String reqName;
	private Socket client;
	private KVServer server;

        RequestThread(String key, String value, String reqName, Socket client, KVServer server) {
            this.key = key;
            this.value = value;
            this.reqName = reqName;
	    this.client = client;
	    this.server = server;
        }

	@Override
        public void run() {
	    KVMessage message = null;
	    try {
		if (reqName.equals(DEL_REQ)) {
		    server.del(key);
		    message = new KVMessage(RESP, SUCCESS);
		} else if (reqName.equals(GET_REQ)) {
		    String keyval = server.get(key);
		    message = new KVMessage(RESP);
		    message.setKey(key);
		    message.setValue(keyval);
		} else if (reqName.equals(PUT_REQ)) {
		    server.put(key, value);
		    message = new KVMessage(RESP, SUCCESS);
		}
	    } catch (KVException e) {
		message = new KVMessage(RESP, e.getKVMessage().getMessage());
	    }
	    try {
		message.sendMessage(client);
	    } catch (KVException e) {}
	    if (!client.isClosed()) {
		try {
		    client.close();
		} catch (IOException e) {}
	    }
	}
    }

}
