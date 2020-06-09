package simpledb;


import com.sun.org.apache.xpath.internal.operations.Bool;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

public class PageLock {
    private final LockManager lockManager;
    private final PageId pageId;
    private final Set<TransactionId> readers;
    private TransactionId writer;
    private final Lock modLock;
    private final Condition modFinished;
    public PageLock(PageId pid, LockManager lockManager){
        this.pageId = pid;
        this.readers = Collections.newSetFromMap(new ConcurrentHashMap<TransactionId, Boolean>());
        this.writer = null;
        this.modLock = new ReentrantLock();
        this.modFinished = this.modLock.newCondition();
        this.lockManager = lockManager;
    }

    public PageId getPageId() {
        return pageId;
    }

    private boolean updateEdgeInDependencyGraph(TransactionId tid, Permissions perm){
        HashSet<TransactionId> holders = new HashSet<TransactionId>();
        if(perm.equals(Permissions.READ_ONLY))
            holders.addAll(this.getWriteHolders());
        else{
            holders.addAll(this.getWriteHolders());
            holders.addAll(this.getReadHolders());
        }
        holders.remove(tid);
        return this.lockManager.updateEdge(tid, holders);
    }

    private void removeEdgeInDependencyGraph(TransactionId tid, Permissions perm){
        HashSet<TransactionId> holders = new HashSet<TransactionId>();
        if(perm.equals(Permissions.READ_ONLY))
            holders.addAll(this.getWriteHolders());
        else{
            holders.addAll(this.getWriteHolders());
            holders.addAll(this.getReadHolders());
        }
        //holders.remove(tid);
        this.lockManager.removeEdge(tid, holders);
    }


    public boolean acquire(TransactionId tid, Permissions perm) throws InterruptedException,
            TransactionAbortedException {
        //this.lock.lock();
        this.modLock.lock();
        //perm = Permissions.READ_WRITE;
        if (perm.equals(Permissions.READ_ONLY)) {
            if (writer == null) {
                readers.add(tid);
                //System.out.println(tid.hashCode() + " get read! size:" + this.readers.size());
                this.modLock.unlock();
                //this.lock.unlock();
                return true;
            } else {
                if (this.writer.equals(tid)) {
                    //readers.add(tid);
                    this.modLock.unlock();
                    //this.lock.unlock();
                    return true;
                } else {
                    while (this.writer != null) {
                        //this.lock.unlock();
                        if(updateEdgeInDependencyGraph(tid, perm)){
                            removeEdgeInDependencyGraph(tid, perm);
                            this.modLock.unlock();
                            //System.out.println( tid.hashCode() + " Dead Lock! - 1");
                            throw new TransactionAbortedException();
                        }
                        this.modFinished.await(2000, TimeUnit.MILLISECONDS);
                        //System.out.println(tid.hashCode() + " loop1");
                        //this.lock.lock();
                    }
                    //System.out.println(tid.hashCode() + " get read! 1");
                    this.readers.add(tid);
                    this.removeEdgeInDependencyGraph(tid, perm);
                    this.modLock.unlock();
                    //this.lock.unlock();
                    return true;
                }
            }
        } else {
            if(this.writer == null){
                if (this.readers.size() == 0) {
                    //System.out.println("reader empty");
                    writer = tid;
                    this.modLock.unlock();
                    //this.lock.unlock();
                    return true;
                } else {
                    //System.out.println("reader not empty");
                    if (this.readers.size() == 1 && this.readers.contains(tid)) {
//                        if(tid.hashCode() == 32){
//                            System.out.print("11-");
//                        }
                        //System.out.println(tid.hashCode() + " get lock here!");
                        writer = tid;
                        this.readers.remove(tid);
                        this.modFinished.signal();
                        this.modLock.unlock();
                        //this.lock.unlock();
                        return true;
                    } else {
//                        System.out.println("shit! in while");
                        while (true) {
                            if (this.readers.size() > 1) {
                                //this.lock.unlock();
                                //System.out.println(tid.hashCode() + " writer is " + writer.hashCode() + " wait!");
                                if(updateEdgeInDependencyGraph(tid, perm)){
                                    removeEdgeInDependencyGraph(tid, perm);
                                    this.modLock.unlock();
                                    //System.out.println( tid.hashCode() + " Dead Lock! - 2");
                                    throw new TransactionAbortedException();
                                }
                                this.modFinished.await(2000, TimeUnit.MILLISECONDS);
                                //System.out.println(tid.hashCode() + " loop2");
                                //this.lock.lock();
                            }
                            else if (this.readers.size() == 1 && !this.readers.contains(tid)) {
                               //this.lock.unlock();
                                if(updateEdgeInDependencyGraph(tid, perm)){
                                    removeEdgeInDependencyGraph(tid, perm);
                                    this.modLock.unlock();
                                    //System.out.println( tid.hashCode() + " Dead Lock! - 3");
                                    throw new TransactionAbortedException();
                                }
                                this.modFinished.await(2000, TimeUnit.MILLISECONDS);
                                //System.out.println(tid.hashCode() + " loop3");
                                //this.lock.lock();
                            }
                            else {
                                break;
                            }
                        }
                        writer = tid;
                        this.removeEdgeInDependencyGraph(tid, perm);
                        this.modLock.unlock();
                        //this.lock.unlock();
                        return true;
                    }
                }
            }
            else{
                if(this.writer.equals(tid)) {
                    this.modLock.unlock();
                    //this.lock.unlock();
                    return true;
                }
                else {
                    while (this.writer != null) {
                        //this.lock.unlock();
                        if(updateEdgeInDependencyGraph(tid, perm)){
                            removeEdgeInDependencyGraph(tid, perm);
                            this.modLock.unlock();
                            //System.out.print( tid.hashCode() + " Dead Lock! - 4");
                            throw new TransactionAbortedException();
                        }
                        this.modFinished.await(2000, TimeUnit.MILLISECONDS);
                        //System.out.println(tid.hashCode() + " loop4");
                        //this.lock.lock();
                    }
                    writer = tid;
                    this.removeEdgeInDependencyGraph(tid, perm);
                    this.modLock.unlock();
                    //this.lock.unlock();
                    return true;
                }
            }
        }
    }

    public boolean releaseAll(TransactionId tid){
        //this.lock.lock();
        this.modLock.lock();
        if(readers.contains(tid)) {
            readers.remove(tid);
            this.modFinished.signal();
        }
        if(writer != null && writer.equals(tid)) {
            //System.out.println(tid.hashCode() + " releaseed!");
            writer = null;
            //this.readFinished.signal();
            this.modFinished.signal();
        }
        this.modLock.unlock();
        //this.lock.unlock();
        return true;
    }

    public boolean releaseRead(TransactionId tid) throws InterruptedException{
        //this.lock.lock();
        this.modLock.lock();
        if(readers.contains(tid)) {
            readers.remove(tid);
            this.modFinished.signal();
        }else{
            throw new InterruptedException("No such tid!");
        }
        this.modLock.unlock();
        //this.lock.unlock();
        return true;
    }

    public boolean releaseWrite(TransactionId tid) throws InterruptedException{
        //this.lock.lock();
        this.modLock.lock();
        if(writer != null) {
            this.writer = null;
            this.modFinished.signal();
        }else{
            throw new InterruptedException("No such tid!");
        }
        this.modLock.lock();
        //this.lock.unlock();
        return true;
    }

    public boolean holdLocks(TransactionId transactionId){
        //this.lock.lock();
        boolean ret = writer.equals(transactionId) || readers.contains(transactionId);
        //this.lock.unlock();
        return ret;
    }

    public boolean holdWrite(TransactionId transactionId){
        //this.lock.lock();
        if(this.writer == null){
           // this.lock.unlock();
            return false;
        }
        else{
            boolean ret =  this.writer.equals(transactionId);
            //this.lock.unlock();
            return ret;
        }
    }

    public HashSet<TransactionId> getWriteHolders(){
        //this.modLock.lock();
        HashSet<TransactionId> ret = new HashSet<TransactionId>();
        if(this.writer != null) ret.add(this.writer);
        //this.modLock.unlock();
        return ret;
    }

    public HashSet<TransactionId> getReadHolders(){
        //this.modLock.lock();
        HashSet<TransactionId> ret = new HashSet<TransactionId>(this.readers);
        //this.modLock.unlock();
        return ret;
    }
}
