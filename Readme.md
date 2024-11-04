**CS525 Advanced Database Organization Assignment 4: B+ Tree**

**Group 29: Srikanth Kalidas Ram Narayan Das, Sanjay Sakthivel, Sailaxman Kotha, Andrew Cook**

**Modules**

Headers: btree_mgr.h, buffer_mgr_stat.h, buffer_mgr.h, dberror.h, dt.h, expr.h, record_mgr.h, storage_mgr.h, tables.h, test_helper.h

C files: btree_mgr.c, buffer_mgr_stat.c, buffer_mgr.c, dberror.c, expr.c, record_mgr.c, rm_serializer.c, storage_mgr.c, test_assign4_1.c, test_expr.c

**Aim**

Our objective was to create a B+ tree that can be used to retrieve the locations of attributes that satisfy a given integer-related schema in log(n) time. The B+ tree handler holds metadata for the B+ tree such as its variable type, and the file the tree references. Page management for the B+ tree includes creating the tree, opening the tree given the file name, closing the tree, and deleting the tree. The tree handler should also be able to access some of the B+ tree's metadata, including the number of nodes, the number of entries, and the key type. Lastly, the tree handler should be able to properly manage key additions and deletions by placing their RID's in the correct leaf node and rebalancing the tree if needed.

**Contributions**

Each group member made an equal contribution to the completion of assignment 4. We were able to learn how a B+ tree is stored within a page file and how to insert, delete, and search along the B+ tree for the RID with a matching key value. Furthermore, we learned how to implement the necessary algorithms to keep the B+ tree self-balancing. Thus, everyone has equal contribution of 25%.

**Contents**

1) Instructions to run code

2) Function Documentation

**1. Instructions to run code**

1. Enter the directory via terminal (assign4_b+tree_index)
2. Enter "make test_assign4_1"
3. Enter "make execute_test1" to run the first test case (test_assign_4_1.c)
4. Enter "make test_expr"
5. Enter "make execute_test2" to run the second test case (test_expr)

**2. Function Documentation**

- **initIndexManager**
    1. Not used

- **shutdownIndexMangger**
    1. Not used

- **createBTree**
    1. Allocate heap space for global treeData variable btreeMt
    2. Allocate heap space for global BTreeHandle variable trHandle
    3. Allocate heap space for global BT_ScanHandle variable scanHandle
    4. Allocate heap space for global scanData variable scanMtdata
    5. Allocate heap space for a buffer pool and assign it to the treeData's bufferManager pointer
    6. Allocate heap space for a page handler and assign it to the treeData's pageHandler pointer
    7. Create the first page of a new file with a given name (idxId) and write it back to the disk
    8. Load the newly created files metadata and store it in the global treeData's file handler pointer
    9. Initialize data for a new B+ tree
        - 1 node
        - root page number is 1
        - 0 entries
        - n (given) maxEntries per page
    10. Create a second page for the new file that will serve as the root of the B+ tree
    11. Reformat B+ tree metadata so it can be read by a char pointer (string)
    12. Write the newly reformatted B+ tree metadata into the first page of our new file
    13. Initialize data for the root node in the B+ tree (located on page number 1)
        - pgNumber = 1
        - leaf = 1
        - parentnode = -1 (because there is parent of the root)
        - numberEntries = 0
    14. Write all dirty pages in the bufferpool back to the disk and shutdown the buffer pool

- **openBtree**
    1. Open the file of the given name and load its metadata into the global treeData's fileHandler
    2. Allocate heap space for the global treeData's pageHandler
    3. Set the bufferpool to hold a maximum of 10 pages from the opened file at once and set the replacement method to first in first out
    4. Load the B+ tree metadata from the first page in the opened file and reformat it so it can be read as a fileMD structure
    5. Copy the newly loaded metadata into the global BTreeHandle
    6. Set the given address to reference the global BTreeHandler 

- **closeBTree**
    1. Reformat the B+ tree metadata so it can be read by a char pointer (string)
    2. Write the reformatted metadata into the first page of the open file
    3. Write all the dirty pages in the bufferpool back to the disk and shutdown the buffer pool
    4. Free heap space taken by the buffer manager
    5. Free heap space taken by the page handler
    6. Free heap space taken by the tree handler

- **deleteBTree**
    1. Delete the file with the given file name

- **getNumNodes**
    1. Retrieve the number of nodes from the B+ tree meta and copy it to the given address

- **getKeyType**
    1. Retrieve the key type from the B+ tree metadata and copy it to the given address

- **getNumEntries**
    1. Retrieve the number of entries from the B+ tree metadata and copy it to the given address

- **getDataBySeperatorForInt**
    1. Get the string the char **ptr is referencing
    2. Allocate space in the heap for a temporary string of 100 characters
    3. Set every character in the temporary string to be a terminating character (\0)
    4. Walk through the given string and copy every character into the temporary string until we come across the given separating character
    5. Convert the temporary strings ascii to an integer
    6. Free the heap space that was taken by the temporary string
    7. Return the created integer

- **readMetaData**
    1. Load the page with the given page number into the bufferpool (if not already there) and pin it
    2. Get the root page number from the loaded page and store it in the given file (B+ tree) metadata structure
    3. Get the number of nodes from the loaded page and store it in the given file (B+ tree) metadata structure
    4. Get the number of entries from the loaded page and store it in the given file (B+ tree) metadata structure
    5. Get the key type from the loaded page and store it in the given file (B+ tree) metadata structure
    6. Unpin the recently pinned page with the given page number

- **getDataBySeperatorForFloat**
    1. Get the string the char **ptr is referencing
    2. Allocate space in the heap for a temporary string of 100 characters
    3. Set every character in the temporary string to be a terminating character (\0)
    4. Walk through the given string and copy every character into the temporary string until we come across the given separating character
    5. Convert the temporary strings ascii to a float
    6. Free the heap space that was taken by the temporary string
    7. Return the created float

- **readPgData**
    1. Load the page with the given page number into the bufferpool (if not already there) and pin it
    2. Get the leaf attribute from the loaded page and store it in the given pgData structure
    3. Get the number of entries from the loaded page and store it in the given pgData structure
    4. Get the parent node from the loaded page and store it in the given pgData structure
    5. Get the page number from the loaded page and store it in the given pgData structure
    6. Allocate heap space for the nodes children
    7. Allocate heap space for each key in the node that was stored in the loaded page
    8. Get all child nodes from the loaded page and store them in a list of floats
    9. Get all keys from the loaded page and store them in a list of integers
    10. Store the list of child nodes into the given pgData structure
    11. Store the list of keys into the given pgData structure
    12. Unpin the recently pinned page with the given page number

- **locatePageToInsertData**
    1. Return the given root page data if the root is a leaf node
    2. If the given key is smaller than the first key in the given node, perform a recursive call with the node the first pointer is referencing
    3. Otherwise, 

- **findKey**
    1. Get the fileHandler from the given tree handler's mgmtData
    2. Get the pageHandler from the given tree handler's mgmtData
    3. Get the buffer pool from the given tree's mgmtData
    4. Get the page number of the B+ tree's root node from the given tree handler's mgmtData
    5. Load the node that contains the given key
    6. Iterate through the node's key values until we find the one with its value equal to the given key
    7. Copy the record slot number to the given RID
    8. Copy the record page number to the given RID

- **insertKey**
    1. Get the page handler from the given tree handler's mgmtData
    2. Get the buffer pool from the given tree handler's mgmtData
    3. Get the file handler from the given tree handler's mgmtData
    4. Get the maximum possible number of entries in a node (page) from the tree handler's mgmtData
    5. Get the number of nodes in the B+ tree from the tree handler's mgmtData
    6. Get the B+ tree's root node's page number from the given tree handler's mgmtData
    7. Load the root node's page into memory with the buffer pool
    8. Get the page of the leaf node that corresponds to the given key value from the B+ tree
    9. 

- **deleteKey**
    1. Get the buffer pool from the given tree handler's mgmtData
    2. Get the page handler from the given tree handler's mgmtData
    3. Get the page number of the B+ tree's root node from the given tree handler's mgmtData
    4. Load the page of the B+ tree's root node into memory
    5. Load the leaf node with a key value matching the given key
    6. Remove the record from the page based on the retrieved RID slot and page number

- **openTreeScan**
    1. Get the buffer pool from the given tree handler's mgmtData
    2. Get the page handler from the given tree handler's mgmtData
    3. Allocate space in the heap for a scan manager
    4. Allocate space in the heap for a scan handler
    5. Get the page number of the B+ tree's root node from the given tree handler's mgmtData
    6. Read the contents of the root node's page and copy it into a pgData structure
    7. Allocate space in the heap for leap page numbers
    8. 

- **nextEntry**
    1. Get the buffer pool from the given tree handler's mgmtData
    2. Get the page handler from the given tree handler's mgmtData
    3. Get the scan data from the given scan handler's mgmtData
    4. Check if the current page position is greater than or equal to the number of entries in the current page
        5. Return RC_IM_NO_MORE_ENTRIES if there are no more leaf pages to scan
        6. Otherwise, move to the next leaf page
    7. Otherwise, load the current page into memory if it isn't already
    8. 

- **closeTreeScan**
    1. Free space taken by the scan handler's mgmtData
    2. Free space taken by the scan handler
    3. Set the scan handler pointer to NULL
