package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class LockManager {
    private final ConcurrentHashMap<PageId, PageLock> pageLocks;
    private final ConcurrentHashMap<TransactionId, Set<PageLock>> pageLockSetOfTransaction;
    private final ConcurrentHashMap<String, LinkedBlockingQueue<String>> dependencyGraph;
    public LockManager(){
        this.pageLocks = new ConcurrentHashMap<PageId, PageLock>();
        this.pageLockSetOfTransaction = new ConcurrentHashMap<TransactionId, Set<PageLock>>();
        this.dependencyGraph = new ConcurrentHashMap<>();
    }
    public boolean lock(TransactionId transactionId, PageId pageId, Permissions perm) throws DbException,
            TransactionAbortedException{
        this.pageLocks.putIfAbsent(pageId, new PageLock(pageId, this));
        PageLock pageLock = this.pageLocks.get(pageId);
        try {
            pageLock.acquire(transactionId, perm);
            //System.out.println(tid_s.toString() + " aquire " + pageId.toString() + " rw");
            pageLockSetOfTransaction.putIfAbsent(transactionId,
                    Collections.newSetFromMap(new ConcurrentHashMap<PageLock, Boolean>()));
            pageLockSetOfTransaction.get(transactionId).add(pageLock);
            return true;
        }catch (InterruptedException e){
            throw new DbException("Interrupted.");
        }

    }

    private boolean dfsDeadLockDetection(String st, Stack<String> dfsStack, HashSet<String> visited){
        visited.add(st);
        if(this.dependencyGraph.containsKey(st)) {
            LinkedBlockingQueue<String> neighbour = this.dependencyGraph.getOrDefault(st, new LinkedBlockingQueue<String>());
            for (String s : neighbour) {
                if (dfsStack.contains(s))
                    return true;
                if (!visited.contains(s)) {
                    dfsStack.push(st);
                    if (dfsDeadLockDetection(s, dfsStack, visited))
                        return true;
                    dfsStack.pop();
                }
            }
            return false;
        }else return false;
    }

    public boolean updateEdge(TransactionId tid, HashSet<TransactionId> toAdd){
        String tid_s = "trans_" + tid.hashCode();
        this.dependencyGraph.putIfAbsent(tid_s, new LinkedBlockingQueue<>());
        LinkedBlockingQueue<String> holder = this.dependencyGraph.get(tid_s);
        for(TransactionId t: toAdd){
            String s = "trans_" + t.hashCode();
            if(!holder.contains(s))
                holder.add(s);
        }
        HashSet<String> visited = new HashSet<String>();
        for(String s : this.dependencyGraph.keySet()){
            if(!visited.contains(s)) {
                if (dfsDeadLockDetection(s, new Stack<>(), visited)) return true;
            }
        }
        return false;
    }


    public void removeTrans(TransactionId trans){
        String s = "trans_" + trans.hashCode();
        synchronized (this.dependencyGraph){
            this.dependencyGraph.remove(s);
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

    public void transactionFinished(TransactionId transactionId){
        if (this.pageLockSetOfTransaction.containsKey(transactionId)) {
            for (PageLock p : this.pageLockSetOfTransaction.get(transactionId)) {
                p.releaseAll(transactionId);
            }
        }
        this.pageLockSetOfTransaction.remove(transactionId);
    }

}