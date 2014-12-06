package kvstore;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.Queue;
import java.util.LinkedList;

public class ThreadPool {

    /* Array of threads in the threadpool */
    public Thread threads[];
    private Lock jobLock;
    private Condition condVar;
    private Queue<Runnable> jobQueue;

    /**
     * Constructs a Threadpool with a certain number of threads.
     *
     * @param size number of threads in the thread pool
     */
    public ThreadPool(int size) {
        threads = new Thread[size];
	jobLock = new ReentrantLock();
	condVar = jobLock.newCondition();
	jobQueue = new LinkedList<Runnable>();
	for (int i = 0; i < size; i++) {
	    threads[i] = this.new WorkerThread(this);
	    threads[i].start();
	}
    }

    /**
     * Add a job to the queue of jobs that have to be executed. As soon as a
     * thread is available, the thread will retrieve a job from this queue if
     * if one exists and start processing it.
     *
     * @param r job that has to be executed
     * @throws InterruptedException if thread is interrupted while in blocked
     *         state. Your implementation may or may not actually throw this.
     */
    public void addJob(Runnable r) {
	jobLock.lock();
	jobQueue.add(r);
	condVar.signal();
	jobLock.unlock();
    }

    /**
     * Block until a job is present in the queue and retrieve the job
     * @return A runnable task that has to be executed
     * @throws InterruptedException if thread is interrupted while in blocked
     *         state. Your implementation may or may not actually throw this.
     */
    public Runnable getJob() throws InterruptedException {
	jobLock.lock();
	while (jobQueue.isEmpty()) {
	    condVar.await();
	}
	Runnable run = jobQueue.remove();
	jobLock.unlock();
	return run;
    }
    
    public void cleanup()
    {
        for (int i = 0; i < threads.length; i++) {
            ((WorkerThread)threads[i]).stopThread();
        }
    }

    /**
     * A thread in the thread pool.
     */
    public class WorkerThread extends Thread {

        public ThreadPool threadPool;
        public boolean stopped = false;
        
        /**
         * Constructs a thread for this particular ThreadPool.
         *
         * @param pool the ThreadPool containing this thread
         */
        public WorkerThread(ThreadPool pool) {
            threadPool = pool;
        }

        /**
         * Scan for and execute tasks.
         */
        @Override
        public void run() {
            while (!stopped) {
                try {
                    threadPool.getJob().run();
                } catch (InterruptedException e) {}
            }
        }
        
        public void stopThread()
        {
            stopped = true;
        }
    }
}









