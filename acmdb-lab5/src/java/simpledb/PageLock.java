package simpledb;


import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PageLock {
    private final LockManager lockManager;
    private final PageId pageId;
    private final Set<TransactionId> readers;
    private TransactionId writer;
    private final Object lock;
    public PageLock(PageId pid, LockManager lockManager){
        this.pageId = pid;
        this.readers = Collections.newSetFromMap(new ConcurrentHashMap<TransactionId, Boolean>());
        this.writer = null;
        this.lockManager = lockManager;
        this.lock = new Object();
    }

    public PageId getPageId() {
        return pageId;
    }

    private boolean acquireReadOnly(TransactionId tid) throws InterruptedException,
            TransactionAbortedException{
        if(this.readers.contains(tid)) {
            return true;
        }
        else{
            while(true){
                synchronized (this.lock) {
                    if (this.writer == null || this.writer.equals(tid)) {
                        lockManager.removeTrans(tid);
                        this.readers.add(tid);
                        //this.modLock.unlock();
                        return true;
                    } else {
                        HashSet<TransactionId> holders = new HashSet<>();
                        if (!this.writer.equals(tid))
                            holders.add(this.writer);
                        if (holders.size() > 0) {
                            if (this.lockManager.updateEdge(tid, holders)) {
                                lockManager.removeTrans(tid);
                                //this.modLock.unlock();
                                throw new TransactionAbortedException();
                            }
                        }
                    }
                }
            }
        }
    }

    private boolean acquireReadWrite(TransactionId tid) throws InterruptedException,
            TransactionAbortedException{
        if(this.writer != null && this.writer.equals(tid)){
            return true;
        }else {
            while(true) {
                synchronized (this.lock) {
                    if ((this.writer == null && (this.readers.size() == 0 || (this.readers.size() == 1 && this.readers.contains(tid))))
                            || (this.writer != null && this.writer.equals(tid) && ((this.readers.size() == 1 && this.readers.contains(tid))))) {
                        this.lockManager.removeTrans(tid);
                        this.writer = tid;
                        return true;
                    } else {
                        HashSet<TransactionId> holders = new HashSet<>();
                        if (this.writer != null)
                            holders.add(this.writer);
                        holders.addAll(this.readers);
                        holders.remove(tid);
                        if (holders.size() > 0) {
                            if (this.lockManager.updateEdge(tid, holders)) {
                                lockManager.removeTrans(tid);
                                throw new TransactionAbortedException();
                            }
                        }
                    }
                }
            }
        }
    }

    public void acquire(TransactionId tid, Permissions perm) throws InterruptedException,
            TransactionAbortedException {
        if (perm.equals(Permissions.READ_ONLY)) {
            this.acquireReadOnly(tid);
        } else if(perm.equals(Permissions.READ_WRITE)){
            this.acquireReadWrite(tid);
        }else
            throw new TransactionAbortedException();
    }

    public boolean releaseAll(TransactionId tid){
        synchronized (this.lock) {
            readers.remove(tid);
            if (writer != null && writer.equals(tid)) {
                writer = null;
            }
            this.lockManager.removeTrans(tid);
        }
        return true;
    }

    public boolean holdLocks(TransactionId transactionId){
        return writer.equals(transactionId) || readers.contains(transactionId);
    }

}
