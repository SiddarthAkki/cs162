package kvstore;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class ThreadPool {

    /* Array of threads in the threadpool */
    public Thread threads[];
    public Queue<Runnable> jobQueue; //added
    public ReentrantLock queueLock; //added
    /**
     * Constructs a Threadpool with a certain number of threads.
     *
     * @param size number of threads in the thread pool
     */
    public ThreadPool(int size) {
        threads = new Thread[size];
        
        // implement me
        jobQueue = new LinkedList<Runnable>();
        for (int i = 0; i < threads.length; i++)
        {
            threads[i] = new WorkerThread(this);
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
    public synchronized void addJob(Runnable r) throws InterruptedException {
        // implement me
        jobQueue.add(r);
    }

    /**
     * Block until a job is present in the queue and retrieve the job
     * @return A runnable task that has to be executed
     * @throws InterruptedException if thread is interrupted while in blocked
     *         state. Your implementation may or may not actually throw this.
     */
    public Runnable getJob() throws InterruptedException {
        // implement me
        while (true)
        {
            //busy wait
            //need to add synchronization
            //this.wait();
            synchronized (this) {
                
                if (!jobQueue.isEmpty())
                {
                    return jobQueue.remove();
                }
            }
            
        }
        
    }

    /**
     * A thread in the thread pool.
     */
    public class WorkerThread extends Thread {

        public ThreadPool threadPool;

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
            // implement me
            while (true)
            {
                //System.out.println("running");
                try
                {
                    threadPool.getJob().run();
                }
                catch (InterruptedException e)
                {
                    //do something I guess?
                }
            }
        }
    }
}
