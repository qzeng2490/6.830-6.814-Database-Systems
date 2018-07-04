package simpledb;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Author: Qiang Zeng
 * @Date: Created in 下午4:55 2018/7/3
 */
public class TransactionLockMap {
    // 记录所有被tid锁住的pid
    private static Map<TransactionId,Set<PageId>> transactionIdSetMap = new HashMap<>();
    // 每个pid只有一个读写锁
    private static Map<PageId,ReentrantReadWriteLock> pageIdReentrantReadWriteLockMap = new HashMap<>();
    // 记录锁住pid的所有tid 用于升级pid的readlock到writelock
    private static Map<PageId,Set<TransactionId>> pageIdSetMap = new HashMap<>();
    // 记录被tid锁住的DbFile 在插入操作的时候 需要在文件级别加锁
    private static Map<TransactionId,Set<Integer>> transactionIdDbMap = new HashMap<>();
    // 由DbFile的id 映射到它的读写锁
    private static Map<Integer,ReentrantReadWriteLock> dbFileReentrantReadWriteLockMap = new HashMap<>();
    private static ReentrantLock lock = new ReentrantLock();
    private TransactionLockMap() {}
    public static Map<TransactionId,Set<PageId>> getTransactionIdSetMap() {
        return transactionIdSetMap;
    }

    public static void shareLock(TransactionId tid, PageId pageId) {
        lock.lock();
        try {
            initMap(tid,pageId);
            ReentrantReadWriteLock l = pageIdReentrantReadWriteLockMap.get(pageId);
//            System.out.println("l.getReadLockCount(): "+l.getReadLockCount());
//            System.out.println("l.getWriteHoldCount(): "+l.getWriteHoldCount());
            if (pageIdSetMap.containsKey(pageId) && pageIdSetMap.get(pageId).size() == 1
                    && pageIdSetMap.get(pageId).iterator().next().equals(tid) && l.isWriteLocked()) {
                // 不要加读锁了  因为只有一个tid对它加了写锁
            }else {
                l.readLock().lock();
            }
        }finally {
            lock.unlock();
        }
//        System.out.println("finish shareLock.Tid: "+tid.getId()+"Pid: "+ pageId.getPageNumber());
    }
    public static void exclusiveLock(TransactionId tid, PageId pageId) {

        lock.lock();
        try {
            initMap(tid,pageId);
//            System.out.println("before writeLock().lock()");
            ReentrantReadWriteLock l = pageIdReentrantReadWriteLockMap.get(pageId);
            // 只有当前的tid 锁住了pageId 可已升级到writelock
            if (pageIdSetMap.containsKey(pageId) && pageIdSetMap.get(pageId).size() == 1
                    && pageIdSetMap.get(pageId).iterator().next().equals(tid) && !l.isWriteLocked()
                    && l.getReadLockCount() > 0) {
                System.out.println("upgrade Read Lock");
//                l.readLock().unlock();
            }
            l.writeLock().lock();
        }finally {
            lock.unlock();
        }
    }
    // 只会在新增page的时候用来加DbFile的写锁
    public static void dbFileLock(TransactionId tid,DbFile dbFile) {
        lock.lock();
        Integer dbId = dbFile.getId();
        try {
            if (transactionIdDbMap.containsKey(tid)) {
                Set<Integer> set = transactionIdDbMap.get(tid);
                if (!set.contains(dbId)) {
                    set.add(dbId);
                }
            } else {
                HashSet<Integer> set = new HashSet();
                set.add(dbId);
                transactionIdDbMap.put(tid,set);
            }
            if (!dbFileReentrantReadWriteLockMap.containsKey(dbId)) {
                dbFileReentrantReadWriteLockMap.put(dbId,new ReentrantReadWriteLock());
            }
            dbFileReentrantReadWriteLockMap.get(dbId).writeLock().lock();
        }finally {
            lock.unlock();
        }
    }

    public static void releaseDbFile(TransactionId tid,DbFile dbFile) {
        lock.lock();
        Integer dbId = dbFile.getId();
        try {
            if (transactionIdDbMap.containsKey(tid) && dbFileReentrantReadWriteLockMap.containsKey(dbId)) {
                ReentrantReadWriteLock rwl = dbFileReentrantReadWriteLockMap.get(dbId);
                if (rwl.isWriteLocked()) {
                    rwl.writeLock().unlock();
                }
            }

        }finally {
            lock.unlock();
        }
    }

    private static void initMap(TransactionId tid, PageId pageId) {
        if (transactionIdSetMap.containsKey(tid)) {
            Set<PageId> set = transactionIdSetMap.get(tid);
            if (!set.contains(pageId)) {
                set.add(pageId);
            }
        } else {
            HashSet<PageId> set = new HashSet();
            set.add(pageId);
            transactionIdSetMap.put(tid,set);
        }
        if (!pageIdReentrantReadWriteLockMap.containsKey(pageId)) {
            pageIdReentrantReadWriteLockMap.put(pageId,new ReentrantReadWriteLock());
        }
        if (pageIdSetMap.containsKey(pageId)) {
            Set<TransactionId> set = pageIdSetMap.get(pageId);
            if (!set.contains(tid)) {
                set.add(tid);
            }
        }else {
            HashSet<TransactionId> set = new HashSet<>();
            set.add(tid);
            pageIdSetMap.put(pageId,set);
        }
    }
    public static boolean holdsLock(TransactionId tid, PageId pageId) {
        lock.lock();
        try {
            if (transactionIdSetMap.containsKey(tid) && pageIdReentrantReadWriteLockMap.containsKey(pageId)) {
                ReentrantReadWriteLock rwl = pageIdReentrantReadWriteLockMap.get(pageId);
                return rwl.isWriteLocked() || rwl.getReadLockCount() >0;
            }
            return false;
        }finally {
            lock.unlock();
        }
    }
    public static void releasePage(TransactionId tid, PageId pageId) {
        lock.lock();
        try {
            if (transactionIdSetMap.containsKey(tid) && pageIdReentrantReadWriteLockMap.containsKey(pageId)) {
                ReentrantReadWriteLock rwl = pageIdReentrantReadWriteLockMap.get(pageId);
                if (rwl.isWriteLocked()) {
                    rwl.writeLock().unlock();
                }
                if (rwl.getReadLockCount() > 0) {
                    rwl.readLock().unlock();
                }
            }
            pageIdSetMap.remove(pageId);

        }finally {
            lock.unlock();
        }
    }

    /**
     * @Author: Qiang Zeng
     * @param tid
     * @Date: 下午8:09 2018/7/3
     * @return void
     * 释放事务tid所有的读写锁
     */
    public static void transactionComplete(TransactionId tid) {
        lock.lock();
        try {
            // 解锁page Lock
            if (transactionIdSetMap.containsKey(tid)) {
                for (PageId pageId: transactionIdSetMap.get(tid)) {
                    if (pageIdReentrantReadWriteLockMap.containsKey(pageId)) {
                        ReentrantReadWriteLock rwl = pageIdReentrantReadWriteLockMap.get(pageId);
                        if (rwl.isWriteLocked()) {
                            rwl.writeLock().unlock();
                        }
                        if (rwl.getReadLockCount() > 0) {
                            rwl.readLock().unlock();
                        }
                    }
                }
            }
            // 解锁 dbFile Lock
            if (transactionIdDbMap.containsKey(tid)) {
                for (Integer fileId: transactionIdDbMap.get(tid)) {
                    if (dbFileReentrantReadWriteLockMap.containsKey(fileId)) {
                        ReentrantReadWriteLock rwl = dbFileReentrantReadWriteLockMap.get(fileId);
                        if (rwl.isWriteLocked()) {
                            rwl.writeLock().unlock();
                        }
                    }
                }
            }
            transactionIdSetMap.remove(tid);
            transactionIdDbMap.remove(tid);
            for (Set<TransactionId> set: pageIdSetMap.values()) {
                set.remove(tid);
            }
        }finally {
            lock.unlock();
        }
    }
}
