package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;


import java.net.Socket;
/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 */
public class TPCMasterHandler implements NetworkHandler {

    public long slaveID;
    public KVServer kvServer;
    public TPCLog tpcLog;
    public ThreadPool threadpool;

    // implement me

    /**
     * Constructs a TPCMasterHandler with one connection in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log) {
        this(slaveID, kvServer, log, 1);
    }

    /**
     * Constructs a TPCMasterHandler with a variable number of connections
     * in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     * @param connections the number of connections in this slave's ThreadPool
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log, int connections) {
        this.slaveID = slaveID;
        this.kvServer = kvServer;
        this.tpcLog = log;
        this.threadpool = new ThreadPool(connections);
    }

    /**
     * Registers this slave server with the master.
     *
     * @param masterHostname
     * @param server SocketServer used by this slave server (which contains the
     *               hostname and port this slave is listening for requests on
     * @throws KVException with ERROR_INVALID_FORMAT if the response from the
     *         master is received and parsed but does not correspond to a
     *         success as defined in the spec OR any other KVException such
     *         as those expected in KVClient in project 3 if unable to receive
     *         and/or parse message
     */
    public void registerWithMaster(String masterHostname, SocketServer server)
            throws KVException {
	Socket master = null;
	Socket listener = null;
	try {
	    master = new Socket(masterHostname, 9090);
	    listener = new Socket(server.getHostname(), server.getPort());
	} catch (IOException e) {
	    throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
	}
	String repr = "";
	repr += this.slaveID + "@" + server.getHostname() + ":" + server.getPort();
	KVMessage message = new KVMessage(REGISTER, repr);
	message.sendMessage(master);
	KVMessage response = new KVMessage(listener);
	String masterResponse = response.getMessage();
	//PARSE MESSAGE!!!!!! INCOMPLETE!!!!!!!!!
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param master Socket connected to the master with the request
     */
    @Override
    public void handle(Socket master) {
        // implement me
        KVMessage message;
        try {
            message = new KVMessage(master);
        } catch (KVException e) {
            message = new KVMessage(ABORT, e.getKVMessage().getMessage());
            try {
                message.sendMessage(master);
            } catch (KVException f) {}
            try {
                master.close();
            } catch (IOException g) {}
            return;
        }

        threadpool.addJob(this.new RequestThread(message, master));

    }

    private class RequestThread implements Runnable {
        private Socket master;
	private KVMessage request;

	RequestThread(KVMessage request, Socket master) {
	    this.request = request;
	    this.master = master;
        }

        @Override
        public void run() {
	    KVMessage response;
	    try {
		//Not TPC Del
		if (reqName.equals(DEL_REQ)) {
		    server.del(key);
		    message = new KVMessage(RESP, SUCCESS);
		} else if (reqName.equals(GET_REQ)) {
		    String keyval = server.get(key);
		    message = new KVMessage(RESP);
		    message.setKey(key);
		    message.setValue(keyval);
		    //Not TPC Put
		} else if (reqName.equals(PUT_REQ)) {
		    server.put(key, value);
		    message = new KVMessage(RESP, SUCCESS);
		}
	    } catch (KVException e) {
		message = new KVMessage(RESP, e.getKVMessage().getMessage());
	    }
	    try {
		message.sendMessage(master);
	    } catch (KVException e) {}
	    try {
		master.close();
	    } catch (IOException e) {}
	}
    }
}
