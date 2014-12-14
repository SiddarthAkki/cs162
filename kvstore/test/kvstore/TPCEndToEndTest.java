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
            client.put(KEY1, "dawg");
        } catch (KVException e) {
            fail("should not have raised exception");
        }
        String val = null;
        try {
            val = client.get(KEY1);
        } catch (KVException e) {
            fail("should not have raised exception");
        }
        assertNotNull(val);
        assertTrue(val.equals("dawg"));
        
        try {
            client.put(KEY1, "newdawg");
        } catch (KVException e) {
            fail("should not have raised exception");
        }
        try {
            val = client.get(KEY1);
        } catch (KVException e) {
            fail("should not have raised exception");
        }
        assertNotNull(val);
        assertTrue(val.equals("newdawg"));
    }
    
    
    @Test(timeout = kTimeoutQuick)
    public void testDel() {
        try {
            client.put(KEY2, "EXTREME!!!!");
        } catch (KVException e) {
            fail("should not have raised exception");
        }
        try {
            client.del(KEY2);
        } catch (KVException e) {
            fail("should not have raised exception");
        }
        try {
            client.get(KEY2);
            fail("Key should have been deleted");
        } catch (KVException e) {
            assertTrue(ERROR_NO_SUCH_KEY.equals(e.getKVMessage().getMessage()));
        }
    }
	
    
    @Test(timeout = kTimeoutQuick)
    public void testErrors() {
        try {
            client.get(KEY3);
            fail("invalid key");
        } catch (KVException e) {
            assertTrue(ERROR_NO_SUCH_KEY.equals(e.getKVMessage().getMessage()));
        }
        
        try {
            client.del(KEY4);
            fail("invalid key");
        } catch (KVException e) {
            assertTrue(ERROR_NO_SUCH_KEY.equals(e.getKVMessage().getMessage()));
        }
        
        try {
            client.put(KEY4, null);
            fail("Need to throw exception on invalid value");
        } catch (KVException e) {
            assertTrue(ERROR_INVALID_VALUE.equals(e.getKVMessage().getMessage()));
        }
        
        try {
            client.put(null, "orchid");
            fail("Need to throw exception on invalid key");
        } catch (KVException e) {
            assertTrue(ERROR_INVALID_KEY.equals(e.getKVMessage().getMessage()));
        }
        
        try {
            client.put(KEY4, "");
            fail("Need to throw exception on invalid value");
        } catch (KVException e) {
            assertTrue(ERROR_INVALID_VALUE.equals(e.getKVMessage().getMessage()));
        }
        
        try {
            client.put("", "orchid");
            fail("Need to throw exception on invalid key");
        } catch (KVException e) {
            assertTrue(ERROR_INVALID_KEY.equals(e.getKVMessage().getMessage()));
        }
        
        try {
            client.get("");
            fail("Need to throw exception on invalid key");
        } catch (KVException e) {
            assertTrue(ERROR_INVALID_KEY.equals(e.getKVMessage().getMessage()));
        }
    }
    
    
}














