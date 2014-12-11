package kvstore;

import static kvstore.KVConstants.*;

import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

public class TPCMaster {

    public int numSlaves;
    public KVCache masterCache;
    public TreeMap<Long, TPCSlaveInfo> slaves;

    public static final int TIMEOUT = 3000;

    ReentrantLock slaveLock;

    boolean fullyRegistered = false;

    Condition slaveCondition;




    /**
     * Creates TPCMaster, expecting numSlaves slave servers to eventually register
     *
     * @param numSlaves number of slave servers expected to register
     * @param cache KVCache to cache results on master
     */
    public TPCMaster(int numSlaves, KVCache cache) {
        this.numSlaves = numSlaves;
        this.masterCache = cache;
        this.slaveLock = new ReentrantLock();
        this.slaveCondition = slaveLock.newCondition();
	this.slaves = new TreeMap<Long, TPCSlaveInfo>(new Comparator<Long>() {
		public int compare(Long first, Long second) {
		    boolean comp1 = TPCMaster.isLessThanUnsigned(first, second);
		    boolean comp2 = TPCMaster.isLessThanEqualUnsigned(first, second);
		    if (comp1) {
			return -1;
		    } else if (comp2) {
			return 0;
		    } else {
			return 1;
		    }
		}
	    });
    }

    /**
     * Registers a slave. Drop registration request if numSlaves already
     * registered. Note that a slave re-registers under the same slaveID when
     * it comes back online.
     *
     * @param slave the slaveInfo to be registered
     */
    public void registerSlave(TPCSlaveInfo slave) {
        slaveLock.lock();

        if (this.slaves.containsKey(slave.getSlaveID()) || this.slaves.size() < this.numSlaves) {
	       this.slaves.put(slave.getSlaveID(), slave);
        }

        if (slaves.size() >= this.numSlaves)
        {
            this.fullyRegistered = true;
            slaveCondition.signalAll();
        }
         
        slaveLock.unlock();
    }

    /**
     * Converts Strings to 64-bit longs. Borrowed from http://goo.gl/le1o0W,
     * adapted from String.hashCode().
     *
     * @param string String to hash to 64-bit
     * @return long hashcode
     */
    public static long hashTo64bit(String string) {
        long h = 1125899906842597L;
        int len = string.length();

        for (int i = 0; i < len; i++) {
            h = (31 * h) + string.charAt(i);
        }
        return h;
    }

    /**
     * Compares two longs as if they were unsigned (Java doesn't have unsigned
     * data types except for char). Borrowed from http://goo.gl/QyuI0V
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than unsigned n2
     */
    public static boolean isLessThanUnsigned(long n1, long n2) {
        return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
    }

    /**
     * Compares two longs as if they were unsigned, uses isLessThanUnsigned
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than or equal to unsigned n2
     */
    public static boolean isLessThanEqualUnsigned(long n1, long n2) {
        return isLessThanUnsigned(n1, n2) || (n1 == n2);
    }

    /**
     * Find primary replica for a given key.
     *
     * @param key String to map to a slave server replica
     * @return SlaveInfo of first replica
     */
    public TPCSlaveInfo findFirstReplica(String key) {
    slaveLock.lock();
	long keyhash = TPCMaster.hashTo64bit(key);
	Long replicaKey = this.slaves.higherKey(keyhash);
	if (replicaKey == null) {
	    replicaKey = this.slaves.firstKey();
	}
    TPCSlaveInfo returnInfo = this.slaves.get(replicaKey);
    slaveLock.unlock();
	return returnInfo;
    }

    /**
     * Find the successor of firstReplica.
     *
     * @param firstReplica SlaveInfo of primary replica
     * @return SlaveInfo of successor replica
     */
    public TPCSlaveInfo findSuccessor(TPCSlaveInfo firstReplica) {
    slaveLock.lock();
	Long nextKey = this.slaves.higherKey(firstReplica.getSlaveID());
	if (nextKey == null) {
	    nextKey = this.slaves.firstKey();
	}
    TPCSlaveInfo returnInfo = this.slaves.get(nextKey);
    slaveLock.unlock();
	return returnInfo;
    }

    /**
     * @return The number of slaves currently registered.
     */
    public int getNumRegisteredSlaves() {
	return this.slaves.size();
    }

    /**
     * (For testing only) Attempt to get a registered slave's info by ID.
     * @return The requested TPCSlaveInfo if present, otherwise null.
     */
    public TPCSlaveInfo getSlave(long slaveId) {
	return this.slaves.get(slaveId);
    }

    /**
     * Perform 2PC operations from the master node perspective. This method
     * contains the bulk of the two-phase commit logic. It performs phase 1
     * and phase 2 with appropriate timeouts and retries.
     *
     * See the spec for details on the expected behavior.
     *
     * @param msg KVMessage corresponding to the transaction for this TPC request
     * @param isPutReq boolean to distinguish put and del requests
     * @throws KVException if the operation cannot be carried out for any reason
     */
    public synchronized void handleTPCRequest(KVMessage msg, boolean isPutReq)
            throws KVException {
	TPCSlaveInfo firstSlave = findFirstReplica(msg.getKey());
	TPCSlaveInfo secondSlave = findSuccessor(firstSlave);
	Socket contactFirst = firstSlave.connectHost(TIMEOUT);
	Socket contactSecond = secondSlave.connectHost(TIMEOUT);
	KVMessage firstReply;
	KVMessage secondReply;
	try {
	    msg.sendMessage(contactFirst);
	    firstReply = new KVMessage(contactFirst, TIMEOUT);
	} catch (KVException e) {
	    firstReply = new KVMessage(ABORT, e.getMessage());
	}

	try {
	    msg.sendMessage(contactSecond);
	    secondReply = new KVMessage(contactSecond, TIMEOUT);
	} catch (KVException e) {
	    secondReply = new KVMessage(ABORT, e.getMessage());
	}

	KVMessage globalDecision;
	if (firstReply.getMsgType().equals(ABORT) || secondReply.getMsgType().equals(ABORT)) {
	    globalDecision = new KVMessage(ABORT);
	} else {
	    globalDecision = new KVMessage(COMMIT);
        if (isPutReq)
        {
            Lock masterCacheLock = masterCache.getLock(msg.getKey());
            masterCacheLock.lock();
            masterCache.put(msg.getKey(), msg.getValue());
            masterCacheLock.unlock();
        }
	}
	
	boolean firstAck = false;
	boolean secondAck = false;
	Socket firstPhaseTwoConnection;
	Socket secondPhaseTwoConnection;
	KVMessage firstPhaseTwoReply;
	KVMessage secondPhaseTWoReply;
	while (!firstAck) {
	    try {
        firstSlave = findFirstReplica(msg.getKey());
		firstPhaseTwoConnection = firstSlave.connectHost(TIMEOUT);
		globalDecision.sendMessage(firstPhaseTwoConnection);
		firstPhaseTwoReply = new KVMessage(firstPhaseTwoConnection, TIMEOUT);
        if (!firstPhaseTwoReply.getMsgType().equals(ACK)) {
            System.out.println("Should not occur: Slave did not respond with an ack");
            throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        }
		firstAck = true;
	    } catch (KVException e) {}
	}

	while (!secondAck) {
	    try {
        secondSlave = findSuccessor(firstSlave);
		secondPhaseTwoConnection = secondSlave.connectHost(TIMEOUT);
		globalDecision.sendMessage(secondPhaseTwoConnection);
		firstPhaseTwoReply = new KVMessage(secondPhaseTwoConnection, TIMEOUT);
        if (!firstPhaseTwoReply.getMsgType().equals(ACK)) {
            System.out.println("Should not occur: Slave did not respond with an ack");
            throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        }
		secondAck = true;
	    } catch (KVException e) {}
	}
	if (firstReply.getMsgType().equals(ABORT)) {
	    throw new KVException(firstReply.getMessage());
	}
	if (secondReply.getMsgType().equals(ABORT)) {
	    throw new KVException(secondReply.getMessage());
	}
    }

    /**
     * Perform GET operation in the following manner:
     * - Try to GET from cache, return immediately if found
     * - Try to GET from first/primary replica
     * - If primary succeeded, return value
     * - If primary failed, try to GET from the other replica
     * - If secondary succeeded, return value
     * - If secondary failed, return KVExceptions from both replicas
     *
     * @param msg KVMessage containing key to get
     * @return value corresponding to the Key
     * @throws KVException with ERROR_NO_SUCH_KEY if unable to get
     *         the value from either slave for any reason
     */
    public String handleGet(KVMessage msg) throws KVException {
        // implement me
        //when getting a key from a given set it should be serial requests
        //when getting a key from different sets it should be concurrent requests
    
        String key = msg.getKey();
        Lock masterLock = masterCache.getLock(key);
        masterLock.lock();
        String value = this.masterCache.get(key);
        if ( value != null)
        {
            masterLock.unlock();
            return value;
        } else {
            TPCSlaveInfo firstRep = findFirstReplica(key);
            KVMessage recvMsg;
            try {
              recvMsg = contactSlave(firstRep, key);
              value = recvMsg.getValue();
              if (value != null) {
                this.masterCache.put(key, value);
                masterLock.unlock();
                return value;
              }
            } catch(Exception e) {
            }
            TPCSlaveInfo secondRep = findSuccessor(firstRep);
            try {
              recvMsg = contactSlave(secondRep, key);
              value = recvMsg.getValue();
              if (value != null) {
                this.masterCache.put(key, value);
                masterLock.unlock();
                return value;
              } else {
                masterLock.unlock();
                throw new KVException(KVConstants.ERROR_NO_SUCH_KEY);
              }
            } catch(Exception e) {
              masterLock.unlock();
              throw new KVException(KVConstants.ERROR_NO_SUCH_KEY);
            }
        }
    }

    private KVMessage contactSlave(TPCSlaveInfo slave, String key) throws KVException {
      KVMessage recvMsg;
      Socket slaveSock = slave.connectHost(TIMEOUT);
      KVMessage msg = new KVMessage(GET_REQ);
      msg.setKey(key);
      msg.sendMessage(slaveSock);
      recvMsg = new KVMessage(slaveSock, TIMEOUT);
      slave.closeHost(slaveSock);
      return recvMsg;
    }

}
