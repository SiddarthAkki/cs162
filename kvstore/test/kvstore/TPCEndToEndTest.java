package kvstore;

import static autograder.TestUtils.*;
import static kvstore.KVConstants.*;
import static kvstore.Utils.assertKVExceptionEquals;
import static kvstore.Utils.*;
import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Random;

import kvstore.Utils.ErrorLogger;
import kvstore.Utils.RandomString;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import autograder.AGCategories.AGTestDetails;
import autograder.AGCategories.AG_PROJ3_CODE;

public class TPCEndToEndTest extends TPCEndToEndTemplate {
    
    @Test(timeout = kTimeoutQuick)
    public void testPutandGet() {
        try {
            client.put("sup", "dawg");
        } catch (KVException e) {
            fail("should not have raised exception");
        }
        String val = null;
        try {
            val = client.get("sup");
        } catch (KVException e) {
            fail("should not have raised exception");
        }
        assertNotNull(val);
        assertTrue(val.equals("dawg"));
    }
    
    
    @Test(timeout = kTimeoutQuick)
    public void testDel() {
        try {
            client.put("Anu", "EXTREME!!!!");
        } catch (KVException e) {
            fail("should not have raised exception");
        }
        try {
            client.del("Anu");
        } catch (KVException e) {
            fail("should not have raised exception");
        }
        try {
            client.get("Anu");
            fail("Key should have been deleted");
        } catch (KVException e) {
            assertTrue(ERROR_NO_SUCH_KEY.equals(e.getKVMessage().getMessage()));
        }
    }
	
    /*
    @Test(timeout = kTimeoutQuick)
    public void testErrors() {
        try {
            client.get("ANUISSOEXTREME!!!");
            fail("invalid key");
        } catch (KVException e) {
            assertTrue(ERROR_NO_SUCH_KEY.equals(e.getKVMessage().getMessage()));
        }
        try {
            client.del("PEANUTBUTTERBANANAS");
            fail("invalid key");
        } catch (KVException e) {
            assertTrue(ERROR_NO_SUCH_KEY.equals(e.getKVMessage().getMessage()));
        }
    }
    */
}
