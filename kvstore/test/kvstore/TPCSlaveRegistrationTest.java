package kvstore;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;

import static kvstore.KVConstants.*;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import autograder.AGCategories.AGTestDetails;
import autograder.AGCategories.AG_PROJ4_CODE;

public class TPCSlaveRegistrationTest {

    String hostname;
    File tempFile1;
    File tempFile2;
    TPCMaster master;
    KVClient client;
    ServerRunner masterClientRunner;
    ServerRunner masterSlaveRunner;
    TPCMasterHandler slave1;
    TPCMasterHandler slave2;

    static final int CLIENTPORT = 8888;
    static final int SLAVEPORT = 9090;

    @Before
    public void setUp() throws Exception {
	hostname = InetAddress.getLocalHost().getHostAddress();
	tempFile1 = File.createTempFile("slaveLog1", ".txt");
	tempFile1.deleteOnExit();
	tempFile2 = File.createTempFile("slaveLog2", ".txt");
	tempFile2.deleteOnExit();
    }


    protected void setupMasterAndSlaves() throws Exception {
	master = new TPCMaster(2, new KVCache(1, 4));
	SocketServer clientSocketServer = new SocketServer(hostname, CLIENTPORT);
	clientSocketServer.addHandler(new TPCClientHandler(master));
        masterClientRunner = new ServerRunner(clientSocketServer, "masterClient");
        masterClientRunner.start();
        SocketServer slaveSocketServer = new SocketServer(hostname, SLAVEPORT);
        slaveSocketServer.addHandler(new TPCRegistrationHandler(master));
        masterSlaveRunner = new ServerRunner(slaveSocketServer, "masterSlave");
        masterSlaveRunner.start();
	KVServer server1 = new KVServer(100, 10);
	KVServer server2 = new KVServer(100, 10);
	TPCLog log1 = new TPCLog(tempFile1.getPath(), server1);
	TPCLog log2 = new TPCLog(tempFile2.getPath(), server2);
	slave1 = new TPCMasterHandler(1234, server1, log1);
	slave2 = new TPCMasterHandler(5678, server2, log2);
    }

    protected void stopMaster() throws Exception {
	masterClientRunner.stop();
	masterSlaveRunner.stop();
    }

    @Test(timeout = 10000)
    @Category(AG_PROJ4_CODE.class)
    public void testRegistrationBasic() throws Exception {
	setupMasterAndSlaves();
	SocketServer slavesocket1 = new SocketServer(hostname);
	slavesocket1.connect();
	SocketServer slavesocket2 = new SocketServer(hostname);
	slavesocket2.connect();
	slave1.registerWithMaster(hostname, slavesocket1);
	slave2.registerWithMaster(hostname, slavesocket2);
	Thread.sleep(100);
	assertTrue(master.slaves.get(new Long(1234)) != null);
	assertTrue(master.slaves.get(new Long(5678)) != null);
	stopMaster();
    }

    @Test(timeout = 30000)
    @Category(AG_PROJ4_CODE.class)
    public void testRequestBeforeFullyRegistered() throws Exception {
	setupMasterAndSlaves();
	Socket testSocket = new Socket(hostname, CLIENTPORT);
	KVMessage testPut = new KVMessage(GET_REQ);
	testPut.setKey("hey");
	try {
	    testPut.sendMessage(testSocket);
	    KVMessage receive = new KVMessage(testSocket, 1000);
	    stopMaster();
	    fail("should have timed out");
	} catch (KVException e) {}
	SocketServer slavesocket1 = new SocketServer(hostname);
        slavesocket1.connect();
        SocketServer slavesocket2 = new SocketServer(hostname);
        slavesocket2.connect();
        slave1.registerWithMaster(hostname, slavesocket1);
        slave2.registerWithMaster(hostname, slavesocket2);
	try {
	    KVMessage shouldReceive = new KVMessage(testSocket);
	    assertTrue(shouldReceive.getMsgType().equals(RESP));
	} catch (KVException e) {
	    stopMaster();
	    fail ("should not have thrown exception");
	}
	stopMaster();
    }

    @Test(timeout = 10000)
    @Category(AG_PROJ4_CODE.class)
    public void testReregistration() throws Exception {
	setupMasterAndSlaves();
	SocketServer slavesocket1 = new SocketServer(hostname);
	slavesocket1.connect();
	SocketServer slavesocket2 = new SocketServer(hostname);
	slavesocket2.connect();
	slave1.registerWithMaster(hostname, slavesocket1);
        slave1.registerWithMaster(hostname, slavesocket2);
        Thread.sleep(100);
	TPCSlaveInfo info = master.slaves.get(new Long(1234));
	stopMaster();
	assertTrue(slavesocket2.port == info.port);
	assertTrue(slavesocket1.port != info.port);
    }
}
