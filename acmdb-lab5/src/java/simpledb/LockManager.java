package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private final ConcurrentHashMap<PageId, PageLock> pageLocks;
    private final ConcurrentHashMap<TransactionId, Set<PageLock>> pageLockSetOfTransaction;
    private final HashMap<String, HashSet<String>> dependencyGraph;
    public LockManager(){
        this.pageLocks = new ConcurrentHashMap<PageId, PageLock>();
        this.pageLockSetOfTransaction = new ConcurrentHashMap<TransactionId, Set<PageLock>>();
        this.dependencyGraph = new HashMap<>();
    }
    public boolean lock(TransactionId transactionId, PageId pageId, Permissions perm) throws DbException,
            TransactionAbortedException{
        this.pageLocks.putIfAbsent(pageId, new PageLock(pageId, this));
        PageLock pageLock = this.pageLocks.get(pageId);
        try {
            boolean ret = pageLock.acquire(transactionId, perm);
            //System.out.println(tid_s.toString() + " aquire " + pageId.toString() + " rw");
            pageLockSetOfTransaction.putIfAbsent(transactionId,
                    Collections.newSetFromMap(new ConcurrentHashMap<PageLock, Boolean>()));
            pageLockSetOfTransaction.get(transactionId).add(pageLock);
            return ret;
        }catch (InterruptedException e){
            throw new DbException("Interrupted.");
        }

    }

    public boolean updateEdge(TransactionId tid, HashSet<TransactionId> toAdd){
        synchronized (this.dependencyGraph){
            String tid_s = "trans_" + tid.hashCode();
            this.dependencyGraph.putIfAbsent(tid_s, new HashSet<String>());
            HashSet<String> holder = this.dependencyGraph.get(tid_s);
            holder.clear();
            for(TransactionId t: toAdd){
                holder.add("trans_" + t.hashCode());
            }
            HashSet<String> visited = new HashSet<String>();
            LinkedList<String> queue = new LinkedList<String>();
            queue.push(tid_s);
            while(!queue.isEmpty()){
                String st = queue.pop();
                if(this.dependencyGraph.containsKey(st)) {
                    for (String n : this.dependencyGraph.get(st)) {
                        if (!visited.contains(n)) {
                            visited.add(n);
                            queue.push(n);
                        }
                    }
                    if (visited.contains(tid_s))
                        return true;
                }
            }
            return false;
        }
    }

    public void removeEdge(TransactionId tid, HashSet<TransactionId> toRemove){
        synchronized (this.dependencyGraph){
            String s = "trans_" + tid.hashCode();
            HashSet<String> holder = this.dependencyGraph.get(s);
            for(TransactionId t : toRemove)
                holder.remove("trans_" + t.hashCode());
        }
    }

    public boolean unlock(TransactionId transactionId, PageId pageId){
        if(this.pageLocks.containsKey(pageId)){
            PageLock pageLock = this.pageLocks.get(pageId);
            return pageLock.releaseAll(transactionId);
        }else return false;
    }

    public boolean holdsLock(TransactionId transactionId, PageId pageId){
        return this.pageLocks.containsKey(pageId) && this.pageLocks.get(pageId).holdLocks(transactionId);
    }

    public boolean holdsWrite(TransactionId transactionId, PageId pageId){
        return this.pageLocks.containsKey(pageId) && this.pageLocks.get(pageId).holdWrite(transactionId);
    }

    public void transactionFinished(TransactionId transactionId){
        synchronized (this.pageLockSetOfTransaction) {
            if (this.pageLockSetOfTransaction.containsKey(transactionId)) {
                for (PageLock p : this.pageLockSetOfTransaction.get(transactionId)) {
                    p.releaseAll(transactionId);
                }
            }
            this.pageLockSetOfTransaction.remove(transactionId);
        }
        synchronized (this.dependencyGraph) {
            this.dependencyGraph.remove("trans_" + transactionId.hashCode());
        }
    }

}
