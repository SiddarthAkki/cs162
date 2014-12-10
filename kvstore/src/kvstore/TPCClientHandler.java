package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * It uses a threadPool to ensure that none of it's methods are blocking.
 */
public class TPCClientHandler implements NetworkHandler {

    public TPCMaster tpcMaster;
    public ThreadPool threadPool;

    /**
     * Constructs a TPCClientHandler with ThreadPool of a single thread.
     *
     * @param tpcMaster TPCMaster to carry out requests
     */
    public TPCClientHandler(TPCMaster tpcMaster) {
        this(tpcMaster, 1);
    }

    /**
     * Constructs a TPCClientHandler with ThreadPool of a single thread.
     *
     * @param tpcMaster TPCMaster to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public TPCClientHandler(TPCMaster tpcMaster, int connections) {
	this.tpcMaster = tpcMaster;
	this.threadPool = new ThreadPool(connections);
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore InterruptedExceptions.
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
	    try {
		client.close();
	    } catch (IOException g) {}
	    return;
	}

	threadPool.addJob(this.new TPCRequestRunnable(message, client));
    }
    
    private class TPCRequestRunnable implements Runnable {
	private KVMessage request;
	private Socket client;

	TPCRequestRunnable(KVMessage request, Socket client) {
	    this.request = request;
	    this.client = client;
	}

	@Override
	public void run() {
		tpcMaster.slaveLock.lock();
		if (!tpcMaster.fullyRegistered) {
			tpcMaster.slaveCondition.await();
		}
		tpcMaster.slaveLock.unlock();
	    String reqName = request.getMsgType();
	    KVMessage response = null;
	    try {
		if (reqName.equals(DEL_REQ)) {
		    tpcMaster.handleTPCRequest(request, false);
		    response = new KVMessage(RESP, SUCCESS);
		} else if (reqName.equals(PUT_REQ)) {
		    tpcMaster.handleTPCRequest(request, true);
		    response = new KVMessage(RESP, SUCCESS);
		} else if (reqName.equals(GET_REQ)) {
		    String val = tpcMaster.handleGet(request);
		    response = new KVMessage(RESP);
		    response.setKey(request.getKey());
		    response.setValue(val);
		}
	    } catch (KVException e) {
		response = new KVMessage(RESP, e.getKVMessage().getMessage());
	    }
	    try {
		response.sendMessage(client);
	    } catch (KVException e) {}
	    try {
		client.close();
	    } catch (IOException e) {}
	}
    }

}
