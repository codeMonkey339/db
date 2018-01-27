The initial code base is cloned from https://github.com/CodingCat/sqllitecmu.git, which is contained in the ***first commit***.
I tried to download the tar ball from course site but failed. And I found the repository from google.
I'm sure that this version is up to date, as the 4th assignment mentioned the version.txt and its content matches related files in this code base.
So I will do the project from this code base. If you're searching for the base code, just go to the link above and clone.

--------------
- [x] project 1 Buffer Pool Manager
- [x] project 2 B+Tree
- [x] project 3 Concurrent Control
- [x] project 4 Logging & Recovery

--------------
* project 1 :
    - There's a very similar question on leetcode about lru cache. I can still recall the circumstance the first time I wrote that, haha.
    - The bucket size of extendible hashing is chosen arbitrarily other than computed. It confused me for some time.
* project 2 :
    - The data structure here is double link between child and parent page via page id. It's different from many online resource
introducing B+Tree with single link. It took me quite a while to fix this in redistributing and coalescing of internal nodes.
    - It's clumsy to handle next page link if two leaf page shares no same parent. A double link version is better to my thought.
    - For the ease of implementation, the size of both leaf and internal nodes are picked as greatest even value.
* project 3 :
    - Lock manger's test case is just a sanity check. Although test is passed, there could be something missing in my implementation.
    The idea is that maintains a internal map of tuples and granted locks. Grant lock request by rules of shared and exclusive locks.
    If one request will block, check whether it won't cause a deadlock. Abort requesting transaction(wait-die) if so. Strict 2PL differs
    in unlock procedure on incoming transaction states from 2PL. Strict 2PL can only work on commited and abort state.
    - Crabbing index concurrent control require r/w lock on pages from root to leaf. Given a root node's page is properly locked, it implies
    that later operations on this tree is well protected and no more locking is needed. Even if root node's lock is released during transversing to leaf node.
    The lock on root page serves as a synchronizing point. It took me a while to figure this out. I use atomic to protect root_page_id and
    compare-and-swap to deal with concurrent insertion into empty tree from two threads.
* project 4 :
    - Double buffer. Background thread waits for time-out or intentionally wake-up signal. On resumption, it swaps log_buffer and flush_buffer
    pointers and related size variables. Then it releases locks on log manager and do sequential writing to flush out the log records. Signal
    other thread upon completion. If a blocking flush is in need, wake up the background thread by signal and wait on completion condition variable.
    Actually, it's sufficient to just do swapping of pointers in background thread other than place the swapping codes outside of the thread.
    I didn't test all the three flushing scenarios. It seems OK on the sanity test in log_manager_test.cpp. Future and async is not used here.
    Maybe using them can make the implementation more clear. I'll leave it for now. Got to say this problem is a much better concurrency problem than those on project 3's.
    - Recovery without check point nor undo. Nor with a long log record list that spans more than one log page.
    - NewPage log record. It seems weird at first glance. When a txn create a new table and dbms crash or whatever happened so that the page
    is initialed in memory but not write back to disk, one new page log record is needed. And init method should be called on that page again.
    As page is allocated by diskmanager, it persists between current execution and later recovery. Just re-initialize that page.
    - Many more test cases are needed for verification. There are only two test case. One is to check output, one is on redo-only recovery.
    BufferManager's force flush is not tested. Undo is not tested. And many more functionalities. Testing is really a big issue in dbms development.
--------------

# 15-445 Database Systems
# SQLite Project Source Code

### Build
```
mkdir build
cd build
cmake ..
make
```
Debug mode:

```
cmake -DCMAKE_BUILD_TYPE=Debug ..
make
```

### Testing
```
cd build
make check
```

### Run virtual table extension in SQLite
Start SQLite with:
```
cd build
./bin/sqlite3
```

In SQLite, load virtual table extension with:

```
.load ./lib/libvtable.dylib
```
or load `libvtable.so` (Linux), `libvtable.dll` (Windows)

Create virtual table:  
1.The first input parameter defines the virtual table schema. Please follow the format of (column_name [space] column_type) seperated by comma. We only support basic data types including INTEGER, BIGINT, SMALLINT, BOOLEAN, DECIMAL and VARCHAR.  
2.The second parameter define the index schema. Please follow the format of (index_name [space] indexed_column_names) seperated by comma.
```
sqlite> CREATE VIRTUAL TABLE foo USING vtable('a int, b varchar(13)','foo_pk a')
```

After creating virtual table:  
Type in any sql statements as you want.
```
sqlite> INSERT INTO foo values(1,'hello');
sqlite> SELECT * FROM foo ORDER BY a;
a           b         
----------  ----------
1           hello   
```
See [Run-Time Loadable Extensions](https://sqlite.org/loadext.html) and [CREATE VIRTUAL TABLE](https://sqlite.org/lang_createvtab.html) for further information.

### Virtual table API
https://sqlite.org/vtab.html

### TODO
* update: when size exceed that page, table heap returns false and delete/insert tuple (rid will change and need to delete/insert from index)
* delete empty page from table heap when delete tuple
* implement delete table, with empty page bitmap in disk manager (how to persistent?)
* index: unique/dup key, variable key
