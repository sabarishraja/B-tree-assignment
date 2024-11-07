#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "buffer_mgr_stat.h"
#include "dberror.h"
#include "storage_mgr.h"
#include "btree_mgr.h"
#include "buffer_mgr.h"
#include <unistd.h> 

//************************************Data Structures*************************************

// Meta data of the file
typedef struct fileMD {
    int rootPgNumber;        // Root page number of the tree
    int numberOfNodes;       // Total number of nodes in the tree
    int numberOfEntries;     // Total number of entries in the file
    int keyType;             // Optional value to store the key type
    int maxEntriesPerPage;   // Maximum number of entries per page
} fileMD;

// Structure for storing key data (key, left pointer, and right pointer)
typedef struct data {
    float left;            // Left pointer
    int key;               // The actual key value
    float right;           // Right pointer
} data;

// Structure for managing the tree's data
typedef struct treeData {
    SM_FileHandle fileHandler;      // File handler for the B-tree
    fileMD fMD;                     // File metadata
    BM_PageHandle* pageHandler;     // Page handler
    BM_BufferPool* bufferManager;   // Buffer manager for the pages
} treeData;

// Page Data
typedef struct pageData {
    // Meta Data
    int numberEntries;    // Number of entries in the page
    int leaf;             // 1 if it's a leaf node, 0 otherwise
    int parentnode;       // Parent node identifier
    int pgNumber;         // Page number

    float *pointers;      // Array of n+1 pointers for pages
    int *keys;            // Array of n keys for the page
} pageData;

// Scan Management Data
typedef struct scanData {
    pageData curPageData;  // Current page data being scanned
    int nextPagePosInLeafPages; // Position of the next page in leaf pages
    int curPosInPage;      // Current position in the page
    int noOfLeafPage;      // Total number of leaf pages
    int *leafPage;         // Array of leaf page entries
    int curPage;           // Page number of the current page
    int isCurPageLoaded;   // Flag indicating if current page is loaded
} scanData;

// Global Variables for handling scans and trees
static int counter = 0;            // Global counter for operations
BT_ScanHandle* scanHandle;         // Scan handle for scanning operations
scanData* scanMtdata;             // Scan metadata
BTreeHandle* trHandle;            // Tree handle for accessing the B-tree
treeData* btreeMt;                // B-tree management data


//****************************************************************************************


//************************************Helper method prototype*****************************

// File and Metadata operations
RC readMetaData(BM_BufferPool* bm, BM_PageHandle* ph, fileMD* fmd, int pageNumber); 
RC prepareMetaData(fileMD* fmd, char* content);
RC pageWrite(BM_BufferPool* bm, BM_PageHandle* ph, char* content, int pageNumber);

// Page and Key operations
RC readpageData(BM_BufferPool* bufferManager, BM_PageHandle* pageHandler, pageData* pageData, int pageNumber);
pageData locatePageToInsertData(BM_BufferPool* bufferManager, BM_PageHandle* pageHandler, pageData root, int key);
RC newkeyAndPtrToLeaf(pageData* pageData, int key, RID rid);
RC keyAndPointerDeletingInLeaf(pageData* pg, int key);
RC insertKeyAndPointerInNonLeaf(pageData* page, data kd);

// Page and Formatting operations
RC formatDataofkeyandPtr(pageData* pd, char* data);
RC preparePageDataToWrite(pageData* pd, char* content);
RC allocateSpace(char **data);
RC deallocateSpace(char **data);

// Helper functions
int getDataBySeperatorForInt(char **ptr, char c);
float getDataBySeperatorForFloat(char **ptr, char c);

// Tree propogation and Updates
RC propagateUp(BTreeHandle *tree, int pageNumber, data kd);
RC updateChildNodesOfParentDown(BTreeHandle* tree, pageData node);


// Leaf Page Retrieval
RC getLeafPg(pageData root, BM_BufferPool* bm, BM_PageHandle* ph, int* leafPages);


//****************************************************************************************


//************************************Initiate and Shutdown*******************************

extern RC initIndexManager (void *mgmtData){
    printf("Index Manager Intialized");
    return RC_OK;
}

extern RC shutdownIndexManager (){
    return RC_OK;
}

//****************************************************************************************


//************************************create, destroy, open, and close an btree index*****

extern RC createBtree(char *idxId, DataType keyType, int maxEntriesPerPage) {
   // Combine multiple structures in a single allocation
btreeMt = (treeData *)malloc(sizeof(treeData) + sizeof(BTreeHandle) + sizeof(BT_ScanHandle) + sizeof(scanData));

    // Assign values directly
    btreeMt->bufferManager = MAKE_POOL();
    btreeMt->pageHandler = MAKE_PAGE_HANDLE();
    trHandle = (BTreeHandle *)((char *)btreeMt + sizeof(treeData));
    scanHandle = (BT_ScanHandle *)((char *)btreeMt + sizeof(treeData) + sizeof(BTreeHandle));
    scanHandle->mgmtData = (scanData *)((char *)btreeMt + sizeof(treeData) + sizeof(BTreeHandle) + sizeof(BT_ScanHandle));
    trHandle->mgmtData = btreeMt;


    // Create the page file for the B-tree index
createPageFile(idxId);

// Open the newly created page file and obtain the file handler
int fileStatus = openPageFile(idxId, &(btreeMt->fileHandler));
if (fileStatus != RC_OK) {
    return fileStatus;  // Return error if the page file cannot be opened
}

// Initialize the metadata for the B-tree
btreeMt->fMD.numberOfNodes = 1;
btreeMt->fMD.rootPgNumber = 1;
btreeMt->fMD.numberOfEntries = 0;
btreeMt->fMD.maxEntriesPerPage = maxEntriesPerPage;

// Set up the buffer pool with 10 pages and a FIFO replacement strategy
initBufferPool(btreeMt->bufferManager, idxId, 10, RS_FIFO, NULL);

// Ensure that the file has at least 2 pages, required for the B-tree structure
ensureCapacity(2, &(btreeMt->fileHandler));

// Prepare the metadata and write it to the metadata page
char *metaBuffer = NULL;
allocateSpace(&metaBuffer);
prepareMetaData(&(btreeMt->fMD), metaBuffer);

// Write the metadata page to the buffer pool
pageWrite(btreeMt->bufferManager, btreeMt->pageHandler, metaBuffer, 0);

// Deallocate memory used for the metadata buffer
deallocateSpace(&metaBuffer);

// Prepare the root page for initialization
pageData rootPage;
rootPage.pgNumber = 1;        // Assign page number 1 for the root page
rootPage.leaf = 1;            // Mark root as a leaf
rootPage.parentnode = -1;     // Root has no parent node
rootPage.numberEntries = 0;   // Initially, no entries in the root page

// Allocate memory for the root page buffer
char *rootPageBuffer = NULL;
allocateSpace(&rootPageBuffer);

// Prepare root page data and write to buffer
preparePageDataToWrite(&rootPage, rootPageBuffer);
pageWrite(btreeMt->bufferManager, btreeMt->pageHandler, rootPageBuffer, btreeMt->fMD.rootPgNumber);

// Release the allocated memory for the root page buffer
deallocateSpace(&rootPageBuffer);

// Shut down the buffer pool
shutdownBufferPool(btreeMt->bufferManager);

// Return successful operation
return RC_OK;

}

extern RC openBtree(BTreeHandle **tree, char *idxId) {
    RC statusCode = openPageFile(idxId, &(btreeMt->fileHandler));  // Attempt to open the page file

    // Handle possible outcomes with switch-case
    switch (statusCode) {
        case RC_OK:
            // File opened successfully, proceed with initialization
            break;
        default:
            // Return the error code if file open fails
            return statusCode;
    }

    // Initialize buffer pool and page handler for the B-tree
    btreeMt->bufferManager = MAKE_POOL();
    btreeMt->pageHandler = MAKE_PAGE_HANDLE();
    initBufferPool(btreeMt->bufferManager, idxId, 10, RS_FIFO, NULL);

    // Retrieve metadata from page 0
    fileMD metadata;
    readMetaData(btreeMt->bufferManager, btreeMt->pageHandler, &metadata, 0);

    // Transfer metadata to B-tree management structure
    trHandle->idxId = idxId;
    trHandle->keyType = metadata.keyType;
    btreeMt->fMD = metadata;  // Copy all metadata fields at once

    // Assign B-tree management data to the handle and return
    trHandle->mgmtData = btreeMt;
    *tree = trHandle;

    return RC_OK;
}


//Close
// Helper function to write B-tree metadata to disk
void writeBtreeMetadata(treeData *btreeData) {
    char *metaDataBuffer;
    allocateSpace(&metaDataBuffer);
    prepareMetaData(&(btreeData->fMD), metaDataBuffer);
    pageWrite(btreeData->bufferManager, btreeData->pageHandler, metaDataBuffer, 0);
    deallocateSpace(&metaDataBuffer);
}

// Helper function to release resources used by B-tree
void releaseBtreeResources(BTreeHandle *tree) {
    treeData *btreeData = (treeData *)tree->mgmtData;

    // Free all allocated memory
    free(btreeData->bufferManager);
    free(btreeData->pageHandler);
    free(tree->mgmtData);
    free(tree);
}

extern RC closeBtree(BTreeHandle *tree) {
    // Extract buffer manager and B-tree metadata
    treeData *btreeData = (treeData *)tree->mgmtData;
    BM_BufferPool *bufferManager = btreeData->bufferManager;

    // Write metadata to disk
    writeBtreeMetadata(btreeData);

    // Shutdown buffer pool to release disk resources
    shutdownBufferPool(bufferManager);

    // Release allocated memory for B-tree resources
    releaseBtreeResources(tree);

    return RC_OK;
}

//Delete tree
extern RC deleteBtree(char *idxId) {
    // Check if the file exists before attempting deletion
    if (access(idxId, F_OK) != 0) {
        // File does not exist, return an error code
        return RC_FILE_NOT_FOUND;
    }

    // Attempt to remove the file and handle potential errors
    if (remove(idxId) == 0) {
        // File successfully deleted
        return RC_OK;
    } else {
        // Deletion failed, return an error code
        return RC_DELETE_FAILED;
    }
}
//****************************************************************************************


//************************************Access information about a b-tree*******************

typedef enum {
    NUM_NODES,
    NUM_ENTRIES,
    KEY_TYPE
} BTreeMetadataField;

// Helper function to access metadata in BTreeHandle
static inline int getBtreeMetadata(BTreeHandle *tree, BTreeMetadataField field) {
    treeData *data = (treeData *)tree->mgmtData;

    switch (field) {
        case NUM_NODES:
            return data->fMD.numberOfNodes;
        case NUM_ENTRIES:
            return data->fMD.numberOfEntries;
        case KEY_TYPE:
            return data->fMD.keyType;
        default:
            return -1;  // Return an invalid value if the field is not recognized
    }
}

RC getNumNodes(BTreeHandle *tree, int *result) {
    *result = getBtreeMetadata(tree, NUM_NODES);
    return (*result != -1) ? RC_OK : RC_ERROR;
}

RC getNumEntries(BTreeHandle *tree, int *result) {
    *result = getBtreeMetadata(tree, NUM_ENTRIES);
    return (*result != -1) ? RC_OK : RC_ERROR;
}

RC getKeyType(BTreeHandle *tree, DataType *result) {
    *result = (DataType)getBtreeMetadata(tree, KEY_TYPE);
    return (*result != -1) ? RC_OK : RC_ERROR;
}
//****************************************************************************************

//***************************************Helper functions****************************

int getDataBySeperatorForInt(char **ptr, char c) {
    char *tempPtr = *ptr;
    char *val = (char*)malloc(100);
    //printf("\nstart int separator\n");

    memset(val, '\0', 100);

    int i = 0;
    while (*tempPtr != c) {
        val[i] = *tempPtr;
        tempPtr++;
        i++;
    }

    int val2 = atoi(val);
    *ptr = tempPtr;
    free(val);
    
    //printf("\nend int separator\n");

    return val2;
}


RC readMetaData(BM_BufferPool* bufferManager, BM_PageHandle* pageHandler, fileMD* fMD, int pgNumber) {
    // Load metadata from specified page
    if (pinPage(bufferManager, pageHandler, pgNumber) != RC_OK) {
        return RC_ERROR;
    }

    char* cursor = pageHandler->data;

    // Move past initial marker
    cursor++; 

    // Read root page number
    fMD->rootPgNumber = getDataBySeperatorForInt(&cursor, '$');
    cursor++; // Advance to next section

    // Read total node count
    fMD->numberOfNodes = getDataBySeperatorForInt(&cursor, '$');
    cursor++; // Move to next section

    // Read entry count
    fMD->numberOfEntries = getDataBySeperatorForInt(&cursor, '$');
    cursor++; // Move to next section

    // Read maximum entries per page
    fMD->maxEntriesPerPage = getDataBySeperatorForInt(&cursor, '$');
    cursor++; // Proceed to key type section

    // Read key type
    fMD->keyType = getDataBySeperatorForInt(&cursor, '$');

    // Release the page
    unpinPage(bufferManager, pageHandler);

    return RC_OK;
}



float getDataBySeperatorForFloat(char **ptr, char delimiter) {
    char buffer[100] = {0};  // Initialize buffer to hold extracted characters
    char *tempPtr = *ptr;

    // Copy characters into buffer until delimiter is encountered
    int i = 0;
    while (*tempPtr != delimiter && i < sizeof(buffer) - 1) {
        buffer[i++] = *tempPtr++;
    }

    // Update original pointer to the current position
    *ptr = tempPtr;

    // Convert the string in buffer to float and return
    float result = atof(buffer);

    return result;
}


RC readpageData(BM_BufferPool *bufferManager, BM_PageHandle *pageHandler, pageData *pageData, int pageNumber) {
    // Pin the page to the buffer manager
    pinPage(bufferManager, pageHandler, pageNumber);

    // Initialize pointer for parsing page data and skip the initial character
    char *dataPtr = pageHandler->data + 1;

    // Extract the page details from the data buffer
    pageData->leaf = getDataBySeperatorForInt(&dataPtr, '$');
    dataPtr++;
    pageData->numberEntries = getDataBySeperatorForInt(&dataPtr, '$');
    dataPtr++;
    pageData->parentnode = getDataBySeperatorForInt(&dataPtr, '$');
    dataPtr++;
    pageData->pgNumber = getDataBySeperatorForInt(&dataPtr, '$');
    dataPtr++;

    // Allocate memory for child pointers and keys based on the number of entries
    int entryCount = pageData->numberEntries;
    pageData->pointers = malloc((entryCount + 1) * sizeof(float));
    pageData->keys = malloc(entryCount * sizeof(int));

    // Populate the keys and pointers from the buffer
    int idx = 0;
    while (idx < entryCount) {
        pageData->pointers[idx] = getDataBySeperatorForFloat(&dataPtr, '$');
        dataPtr++;
        pageData->keys[idx] = getDataBySeperatorForInt(&dataPtr, '$');
        dataPtr++;
        idx++;
    }
    
    // Set the last pointer if entries are present
    if (entryCount > 0) {
        pageData->pointers[entryCount] = getDataBySeperatorForFloat(&dataPtr, '$');
    }

    // Unpin the page and complete the operation
    unpinPage(bufferManager, pageHandler);

    return RC_OK;
}


pageData locatePageToInsertData(BM_BufferPool *bufferManager, BM_PageHandle *pageHandler, pageData root, int key) {
    // Check if the root is a leaf node; if so, return it directly
    if (root.leaf) {
        return root;
    }

    int pageSearchNumber;
    pageData searchPage;

    // If the key is smaller than the first key in the root, search the first child
    if (key < root.keys[0]) {
        pageSearchNumber = (int)(root.pointers[0]);
        readpageData(bufferManager, pageHandler, &searchPage, pageSearchNumber);
        return locatePageToInsertData(bufferManager, pageHandler, searchPage, key);
    }

    // Otherwise, search for the appropriate child page within the key range
    int foundPage = 0;
    for (size_t i = 0; i < root.numberEntries - 1 && !foundPage; i++) {
        if (key >= root.keys[i] && key < root.keys[i + 1]) {
            pageSearchNumber = (int)(root.pointers[i + 1]);
            readpageData(bufferManager, pageHandler, &searchPage, pageSearchNumber);
            return locatePageToInsertData(bufferManager, pageHandler, searchPage, key);
        }
    }

    // If no appropriate page was found, search the last pointer in the root
    pageSearchNumber = (int)(root.pointers[root.numberEntries]);
    readpageData(bufferManager, pageHandler, &searchPage, pageSearchNumber);
    return locatePageToInsertData(bufferManager, pageHandler, searchPage, key);
}

RC newkeyAndPtrToLeaf(pageData* pageData, int key, RID rid) {
    // Allocate memory for new arrays to store pointers and keys
    float *children = (float*)malloc(10 * sizeof(int));
    int *keys = (int*)malloc(10 * sizeof(int));
    
    // Initial indexing and loop through existing keys to find the correct insertion point
    int idx = 0;
    while (idx < pageData->numberEntries) {
        if (key <= pageData->keys[idx]) break;
        
        // Copy existing keys and pointers up to the insertion point
        keys[idx] = pageData->keys[idx];
        children[idx] = pageData->pointers[idx];
        idx++;
    }

    // Check for duplicate key
    if (idx < pageData->numberEntries && key == pageData->keys[idx]) {
        free(children);
        free(keys);
        return RC_IM_KEY_ALREADY_EXISTS;
    }

    // Calculate and insert new pointer for the given RID
    float cPointer = rid.page + (rid.slot * 0.1);
    keys[idx] = key;
    children[idx] = cPointer;
    idx++;

    // Shift remaining keys and pointers into the new arrays
    for (int shiftIdx = idx; shiftIdx < pageData->numberEntries + 1; shiftIdx++) {
        children[shiftIdx] = pageData->pointers[shiftIdx - 1];
        keys[shiftIdx] = pageData->keys[shiftIdx - 1];
    }

    // Mark the end of pointers
    children[pageData->numberEntries + 1] = -1;

    // Free old memory and assign new arrays to pageData
    free(pageData->keys);
    free(pageData->pointers);
    pageData->keys = keys;
    pageData->pointers = children;
    pageData->numberEntries++;

    return RC_OK;
}


RC allocateSpace(char **data) {
    if (data == NULL) {
        return RC_NULL_POINTER; // Return an error if the data pointer is NULL
    }

    // Dynamically allocate space for 50 characters
    *data = (char *)malloc(50 * sizeof(char));
    
    // Check if memory allocation was successful
    if (*data == NULL) {
        return RC_MEMORY_ALLOCATION_FAILED; // Return an error if malloc fails
    }

    // Initialize allocated space to null characters
    for (int i = 0; i < 50; i++) {
        (*data)[i] = '\0';
    }

    return RC_OK; // Return OK after successful allocation
}


RC deallocateSpace(char **data) {
    // Check if the provided pointer is not NULL
    if (data == NULL || *data == NULL) {
        return RC_NULL_POINTER; // Return error if the data pointer is NULL
    }

    // Free the allocated memory
    free(*data);

    // Set the pointer to NULL to avoid dangling pointer
    *data = NULL;

    return RC_OK; // Indicate successful deallocation
}

RC formatDataofkeyandPtr(pageData* pageData, char* content) {
    // Initialize cursor to point to the beginning of content
    char *cursor = content;

    // Loop over entries in pageData using a for loop
    for (int idx = 0; idx < pageData->numberEntries; idx++) {
        int childValue = round(pageData->pointers[idx] * 10);
        int slot = childValue % 10;
        int pageNum = childValue / 10;

        // Format page and slot data
        sprintf(cursor, "%d.%d$", pageNum, slot);
        cursor += 4;  // Move cursor forward after inserting page and slot

        // Format key and adjust cursor based on key size
        sprintf(cursor, "%d$", pageData->keys[idx]);
        cursor += (pageData->keys[idx] >= 10) ? 3 : 2;  // Adjust cursor based on key's digit count
    }

    // Append the final pointer value as a float
    sprintf(cursor, "%0.1f$", pageData->pointers[pageData->numberEntries]);

    return RC_OK;
}


RC prepareMetaData(fileMD* fMD, char* data) {
    // Ensure that the data pointer is valid
    if (data == NULL) {
        return RC_NULL_POINTER;
    }

    // Use snprintf to format the metadata string more safely
    int written = snprintf(data, 100, "$%d$%d$%d$%d$%d$", 
                           fMD->rootPgNumber, 
                           fMD->numberOfNodes, 
                           fMD->numberOfEntries, 
                           fMD->maxEntriesPerPage, 
                           fMD->keyType);

    // Check if the snprintf was successful
    if (written < 0) {
        return RC_FORMATTING_ERROR;  // Return an error if snprintf fails
    }

    // Optionally check if the written data fits within the allocated buffer size
    if (written >= 100) {
        return RC_BUFFER_OVERFLOW;  // Buffer overflow protection
    }

    return RC_OK;
}

RC preparePageDataToWrite(pageData* pageData, char* content) {
    // Check for null pointers to avoid dereferencing invalid memory
    if (content == NULL || pageData == NULL) {
        return RC_NULL_POINTER;
    }

    // Start by formatting the basic page information into the content string
    int written = snprintf(content, 100, "$%d$%d$%d$%d$",
                           pageData->leaf, 
                           pageData->numberEntries, 
                           pageData->parentnode, 
                           pageData->pgNumber);

    // Check if snprintf was successful
    if (written < 0) {
        return RC_FORMATTING_ERROR;
    }

    // If the page has entries, append the keys and pointers to the content
    if (pageData->numberEntries > 0) {
        char* keysAndPointers = NULL;

        // Allocate memory for the keys and pointers data
        RC allocationStatus = allocateSpace(&keysAndPointers);
        if (allocationStatus != RC_OK) {
            return allocationStatus; // Return if allocation fails
        }

        // Format the key-pointer data
        formatDataofkeyandPtr(pageData, keysAndPointers);

        // Append the formatted key-pointer data to the content
        strcat(content, keysAndPointers);

        // Deallocate the memory after use
        deallocateSpace(&keysAndPointers);
    }

    // Return success after successfully formatting the page data
    return RC_OK;
}

RC pageWrite(BM_BufferPool* bufferManager,BM_PageHandle* pageHandler,char* content,int pageNumber){
    //printf("page write\n");
    // Pin the page with specified index to modify its contents
    pinPage(bufferManager,pageHandler,pageNumber);
    //printf("page pinned");
    // Clear existing data in the page buffer and set new content
    memset(pageHandler->data,'\0',100); // Clear up to 100 characters
    sprintf(pageHandler->data,"%s",content); // Copy new data into the page
    // Mark the page as dirty since its content has been changed
    markDirty(bufferManager,pageHandler);
    unpinPage(bufferManager,pageHandler); 
}

RC propagateUp(BTreeHandle *treeHandler,int pgNumb,data keyData){

    BM_BufferPool *bufferManager = ((treeData*)treeHandler->mgmtData)->bufferManager;
    BM_PageHandle *pageHandler = ((treeData*)treeHandler->mgmtData)->pageHandler;
    SM_FileHandle fileHandler = ((treeData*)treeHandler->mgmtData)->fileHandler;
    
    int maxEntity = ((treeData*)treeHandler->mgmtData)->fMD.maxEntriesPerPage;
    int curNumOfNodes = ((treeData*)treeHandler->mgmtData)->fMD.numberOfNodes;
    
    // if parent page number is -1, create new node
    if(pgNumb != -1){
        pageData newPageToAdd; // page is full or not full, add data in existing page
        readpageData(bufferManager,pageHandler,&newPageToAdd,pgNumb);
        insertKeyAndPointerInNonLeaf(&newPageToAdd,keyData);
        
        if(newPageToAdd.numberEntries > maxEntity){ // if page have more than max entries
            // if is true, divide and copy the data into new pages, the key of new node will be in right child and old node will be in left child. 
            float *oldNodeChildren = (float *)malloc(10*sizeof(float));
            int count = 0, *oldNodeKeys = (int *)malloc(10*sizeof(int));
            size_t index = 0;
            while(index < (int)ceil((newPageToAdd.numberEntries)/2))
            {
                oldNodeKeys[count] = newPageToAdd.keys[index];
                oldNodeChildren[count] = newPageToAdd.pointers[index];
                count++;
                index++;
            }
            oldNodeChildren[count] = newPageToAdd.pointers[count];
            count++;

            int count2 = 0,*keysForNewNode = (int *)malloc(10*sizeof(int));
            float *childrenForNewNode = (float *)malloc(10*sizeof(float));
            index=count;
            while(index < newPageToAdd.numberEntries + 2)
            {
                keysForNewNode[count2] = newPageToAdd.keys[index];
                childrenForNewNode[count2] = newPageToAdd.pointers[index];
                count2++;
                index++;
            }
            childrenForNewNode[count2] = newPageToAdd.pointers[count];

            ensureCapacity (curNumOfNodes + 2, &fileHandler);

            curNumOfNodes += 1;

            ((treeData*)treeHandler->mgmtData)->fMD.numberOfNodes++;

            //Right child data
            pageData pRChild;
            pRChild.leaf = 0;
            pRChild.pgNumber = curNumOfNodes;
            pRChild.numberEntries = (int)floor((maxEntity+1)/2);
            pRChild.keys = keysForNewNode;
            pRChild.pointers = childrenForNewNode;
            pRChild.parentnode = newPageToAdd.parentnode;

            //data update on right child
            char *dataStr;
            allocateSpace(&dataStr);
            preparePageDataToWrite(&pRChild,dataStr);
            pageWrite(bufferManager,pageHandler,dataStr,pRChild.pgNumber);
            deallocateSpace(&dataStr);

            //Left child Data
            pageData pLChild;
            pLChild.leaf = 0;
            pLChild.pgNumber = newPageToAdd.pgNumber;
            pLChild.numberEntries = (int)floor((maxEntity+1)/2);
            pLChild.keys = oldNodeKeys;
            pLChild.pointers = oldNodeChildren;
            pLChild.parentnode = newPageToAdd.parentnode;

            //Update data on left child
            allocateSpace(&dataStr);
            preparePageDataToWrite(&pLChild,dataStr);
            pageWrite(bufferManager,pageHandler,dataStr,pLChild.pgNumber);
            deallocateSpace(&dataStr);

            int pgNum = newPageToAdd.parentnode;
            float left = pLChild.pgNumber;
            float right = pRChild.pgNumber;

            data kdata;
            kdata.key = newPageToAdd.keys[(int)ceil((maxEntity+1)/2)];
            kdata.left = left;
            kdata.right = right;

            //update childe of each parent
            updateChildNodesOfParentDown(treeHandler,pRChild);

            //propagate up
            propagateUp(treeHandler,pgNum,kdata);

        }
        else{
            char *dataString;
            allocateSpace(&dataString);
            preparePageDataToWrite(&newPageToAdd,dataString);
            pageWrite(bufferManager,pageHandler,dataString,newPageToAdd.pgNumber);
            deallocateSpace(&dataString);
            return RC_OK;   
        }

    }
    else{
        int curNumOfNodes = ((treeData*)treeHandler->mgmtData)->fMD.numberOfNodes;
        ensureCapacity(curNumOfNodes+2,&fileHandler);

        pageData newRoot; // making new node as root
        newRoot.pgNumber = curNumOfNodes+1;

        int *keysofNewRoot = (int *)malloc(10*sizeof(int));
        keysofNewRoot[0] = keyData.key;

        float *childrenofNewRoot = (float *)malloc(10*sizeof(float));
        childrenofNewRoot[0] = keyData.left;
        childrenofNewRoot[1] = keyData.right;

        newRoot.keys = keysofNewRoot;
        newRoot.pointers= childrenofNewRoot;
        newRoot.numberEntries = 1;
        newRoot.parentnode = -1;
        newRoot.leaf = 0;

        ((treeData*)treeHandler->mgmtData)->fMD.numberOfNodes++;
        ((treeData*)treeHandler->mgmtData)->fMD.rootPgNumber = newRoot.pgNumber;

        char *dataString;
        allocateSpace(&dataString);
        preparePageDataToWrite(&newRoot,dataString);
        pageWrite(bufferManager,pageHandler,dataString,newRoot.pgNumber);
        deallocateSpace(&dataString);

        //updating child nodes of each parent
        updateChildNodesOfParentDown(treeHandler,newRoot);
    }
}

RC updateChildNodesOfParentDown(BTreeHandle *tree, pageData dataNode) {
    // Retrieve buffer manager and page handler from the tree structure
    treeData *treeInfo = (treeData *)tree->mgmtData;
    BM_BufferPool *bufferManager = treeInfo->bufferManager;
    BM_PageHandle *pageHandler = treeInfo->pageHandler;

    // Loop through the child pointers of the node to update their parent node
    size_t childIndex = 0;
    while (childIndex <= dataNode.numberEntries) {
        // Fetch the child page using the pointer from the parent node
        pageData childPage;
        readpageData(bufferManager, pageHandler, &childPage, dataNode.pointers[childIndex]);

        // Set the current node as the parent of the child
        childPage.parentnode = dataNode.pgNumber;

        // Prepare and write the updated child page to the buffer pool
        char *pageContent;
        allocateSpace(&pageContent);
        preparePageDataToWrite(&childPage, pageContent);
        pageWrite(bufferManager, pageHandler, pageContent, childPage.pgNumber);
        deallocateSpace(&pageContent);

        // Move to the next child node
        childIndex++;
    }

    return RC_OK;
}
;



RC insertKeyAndPointerInNonLeaf(pageData* pageData,data keyData){
    int *updatedKeys = (int*)malloc(sizeof(int)*10); // Array for new keys
    float *updatedPointers = (float*)malloc(sizeof(int)*10); // Array for new pointers
    int currentPosition = 0;
    while(keyData.key > pageData->keys[currentPosition] && currentPosition < pageData->numberEntries){
        updatedKeys[currentPosition] = pageData->keys[currentPosition];
        updatedPointers[currentPosition] = pageData->pointers[currentPosition];
        currentPosition++;
    }

    if(keyData.key == pageData->keys[currentPosition] && currentPosition < pageData->numberEntries){
        return RC_IM_KEY_ALREADY_EXISTS;
    }
    else{
        updatedKeys[currentPosition] = keyData.key;
        updatedPointers[currentPosition] = keyData.left;
        updatedPointers[currentPosition+1] = keyData.right;
        currentPosition++;
        updatedKeys[currentPosition] = pageData->keys[currentPosition-1];
        currentPosition++;
        
    }

    // for (int j = currentPosition; j < page->numberEntries + 2; j++) {
    // updatedKeys[j] = page->keys[j - 1];
    // updatedPointers[j] = page->pointers[j - 1];
    // }
    while(currentPosition<pageData->numberEntries+2){
        updatedKeys[currentPosition]=pageData->keys[currentPosition-1];
        updatedPointers[currentPosition]=pageData->pointers[currentPosition-1];
        currentPosition++;
    }

    updatedPointers[currentPosition] = -1;
    free(pageData->keys);
    free(pageData->pointers);
    pageData->numberEntries++;
    pageData->pointers = updatedPointers;
    pageData->keys = updatedKeys;
   
    return RC_OK;
}

RC keyAndPointerDeletingInLeaf(pageData* dataPage, int keyToDelete) {
    int tempKeys[5] = {0};          // Temporary array to store keys
    float tempChildren[5] = {0};    // Temporary array to store pointers
    int newKeyCount = 0;            // Counter for new keys
    bool keyFound = false;          // Flag to track if the key is found

    // Loop through existing keys and prepare updated keys/pointers
    for (int i = 0; i < dataPage->numberEntries; i++) {
        if (dataPage->keys[i] == keyToDelete) {
            keyFound = true;  // Set the flag if the key is found
            continue;         // Skip this key and don't copy it to the new arrays
        }
        // Copy keys and pointers that aren't deleted to temporary arrays
        tempKeys[newKeyCount] = dataPage->keys[i];
        tempChildren[newKeyCount] = dataPage->pointers[i];
        newKeyCount++;  // Increment the counter for valid keys
    }

    // If the key is not found, return the error
    if (!keyFound) {
        return RC_IM_KEY_NOT_FOUND;
    }

    // Update the number of entries on the page
    dataPage->numberEntries -= 1;

    // Set the last pointer to an invalid value (-1)
    tempChildren[newKeyCount] = -1;

    // Copy the new keys and pointers back to the page data
    for (int i = 0; i < newKeyCount; i++) {
        dataPage->keys[i] = tempKeys[i];
        dataPage->pointers[i] = tempChildren[i];
    }

    // Update the last pointer after copying the keys and children
    dataPage->pointers[newKeyCount] = tempChildren[newKeyCount];

    return RC_OK;
}


RC getLeafPg(pageData currentPage, BM_BufferPool* bufferManager, BM_PageHandle* pageHandler, int* leafPages) {
    size_t childIdx = 0;  // Index for child nodes
    RC status = RC_OK;  // Variable to track function status

    // Traverse through internal nodes until we find leaf nodes
    while (!currentPage.leaf && childIdx < currentPage.numberEntries + 1) {
        pageData childPage;  // Initialize child page
        int childPageNum = (int)currentPage.pointers[childIdx];  // Get child page number

        // Read child page into memory
        status = readpageData(bufferManager, pageHandler, &childPage, childPageNum);
        if (status != RC_OK) return RC_READ_FAILED;  // Check for read error

        // Recurse into the child page to find leaf nodes
        status = getLeafPg(childPage, bufferManager, pageHandler, leafPages);
        if (status == RC_OK) {
            childIdx++;  // Increment to check the next child if we found a leaf page
        } else {
            childIdx++;  // Increment child index and continue
        }
    }

    // Once a leaf page is found, store its page number
    if (currentPage.leaf) {
        leafPages[counter] = currentPage.pgNumber;
        counter++;  // Increment the leaf page counter
    }

    return RC_OK;
}


//****************************************************************************************


//********************************Index Acess functions***********************************

// to find the given key
RC findKey(BTreeHandle *tree, Value *key, RID *result) {
    // Obtain necessary data structures
    SM_FileHandle fileHandler = ((treeData*)tree->mgmtData)->fileHandler;
    BM_PageHandle *pageHandler = ((treeData*)tree->mgmtData)->pageHandler;
    BM_BufferPool *bufferManager = ((treeData*)tree->mgmtData)->bufferManager;

    pageData rootPageData;  // Root page data

    // Retrieve the root page number
    int rootPageNumber = ((treeData*)tree->mgmtData)->fMD.rootPgNumber;

    // Read the root page into memory
    if (readpageData(bufferManager, pageHandler, &rootPageData, rootPageNumber) != RC_OK) {
        return RC_READ_FAILED;
    }

    // Locate the page where the key should be found
    pageData leafPageData = locatePageToInsertData(bufferManager, pageHandler, rootPageData, key->v.intV);
    
    size_t idx = 0;  // To iterate through entries on the page

    while (1) {
        switch (idx < leafPageData.numberEntries) {
            case 0:  // Exit condition when index is out of bounds
                return RC_IM_KEY_NOT_FOUND;
            default:  // Check for the key in the current entry
                if (leafPageData.keys[idx] == key->v.intV) {
                    float pointer = leafPageData.pointers[idx];
                    int adjustedValue = round(pointer * 10);
                    int pageNumber = adjustedValue / 10;
                    int slotNumber = adjustedValue % 10;

                    // Assign the result
                    result->page = pageNumber;
                    result->slot = slotNumber;

                    return RC_OK;
                }
                break;
        }
        idx++;  // Increment the index for the next iteration
    }
}


RC insertKey (BTreeHandle *tree, Value *key, RID rid) {
    
    //printf("\nstart insert key\n");

    // Getting the page handler, buffer manager and file handler
    treeData *treeMgmtData = (treeData*)tree->mgmtData;

    // Extract the necessary data from the tree management structure
    BM_PageHandle *pageHandler = treeMgmtData->pageHandler;
    BM_BufferPool *bufferManager = treeMgmtData->bufferManager;
    SM_FileHandle fileHandler = treeMgmtData->fileHandler;

    int maxEntry = treeMgmtData->fMD.maxEntriesPerPage;
    int curNumOfNode = treeMgmtData->fMD.numberOfNodes;
    int rootPgNum = treeMgmtData->fMD.rootPgNumber;

    // Initialize pageData structure to hold root page data
    pageData rootPage;

    // Read the root page data
    RC readStatus = readpageData(bufferManager, pageHandler, &rootPage, rootPgNum);
    if (readStatus != RC_OK) {
        // Handle error in reading the page
        return readStatus;
    }

    //printf("read pg data");

    pageData insertionPage; // getting the page where data can be inserted
    insertionPage = locatePageToInsertData(bufferManager, pageHandler, rootPage, key->v.intV);

    //printf("\nlocated page!------------\n");
    
    if (newkeyAndPtrToLeaf(&insertionPage, key->v.intV, rid) == RC_IM_KEY_ALREADY_EXISTS) {
        return RC_IM_KEY_ALREADY_EXISTS;
    }

    //printf("\nnew key and ptr to leaf!------------\n");

    if (insertionPage.numberEntries > maxEntry) { // check if there is no space in the leaf node
        
        // Creating new nodes
        int counter = 0;
        float *newNodechildren = (float *)malloc(10 * sizeof(float));
        int *newNodeKeys = (int *)malloc(10 * sizeof(int));
        
        size_t i = (int)ceil((insertionPage.numberEntries) / 2) + 1;
        while (i < insertionPage.numberEntries) {
            newNodechildren[counter] = insertionPage.pointers[i];
            newNodeKeys[counter] = insertionPage.keys[i];
            counter++;
            i++;
        }
        newNodechildren[counter] = -1;

        counter = 0;
        int *oldNodeKeys = (int *)malloc(10 * sizeof(int));
        float *oldNodeChildren = (float *)malloc(10 * sizeof(float));

        i = 0;
        while (i <= (int)ceil((insertionPage.numberEntries) / 2)) {
            oldNodeKeys[counter] = insertionPage.keys[i];
            oldNodeChildren[counter] = insertionPage.pointers[i];
            counter++;
            i++;
        }
        oldNodeChildren[counter] = -1;

// Ensure capacity for additional nodes and update the node count
ensureCapacity(curNumOfNode + 2, &fileHandler);
curNumOfNode++;
((treeData*)tree->mgmtData)->fMD.numberOfNodes++;

// Configure the right child node
pageData rightChild = {
    .pgNumber = curNumOfNode,
    .leaf = 1,
    .pointers = newNodechildren,
    .numberEntries = (int)floor((maxEntry + 1) / 2),
    .keys = newNodeKeys,
    .parentnode = (insertionPage.parentnode == -1) ? 3 : insertionPage.parentnode
};

//printf("start update right child");

// Update right child node data
char *dataHolder;
allocateSpace(&dataHolder);
preparePageDataToWrite(&rightChild, dataHolder);
pageWrite(bufferManager, pageHandler, dataHolder, rightChild.pgNumber);
deallocateSpace(&dataHolder);
//printf("updated right child");

// Configure the left child node
pageData leftChild = {
    .leaf = 1,
    .pgNumber = insertionPage.pgNumber,
    .numberEntries = (int)ceil((maxEntry + 1) / 2) + 1,
    .keys = oldNodeKeys,
    .pointers = oldNodeChildren,
    .parentnode = (insertionPage.parentnode == -1) ? 3 : insertionPage.parentnode
};

//printf("start update left child");

// Update left child node data
allocateSpace(&dataHolder);
preparePageDataToWrite(&leftChild, dataHolder);
pageWrite(bufferManager, pageHandler, dataHolder, leftChild.pgNumber);
deallocateSpace(&dataHolder);
//printf("updated left child");

// Set up data for upward propagation
int pgNumber = insertionPage.parentnode;
float left = leftChild.pgNumber, right = rightChild.pgNumber;

data keyData = {
    .left = left,
    .key = rightChild.keys[0],
    .right = right
};

//printf("propagate start");
propagateUp(tree, pgNumber, keyData); // propagate up
//printf("propagate end");

} else {
    char *dataHolder = malloc(500);
    //printf("at root--------");
    allocateSpace(&dataHolder);
    preparePageDataToWrite(&insertionPage, dataHolder);
    pageWrite(bufferManager, pageHandler, dataHolder, insertionPage.pgNumber);
    deallocateSpace(&dataHolder);
    //printf("done root");
}

// Update entry count and flush buffer pool
((treeData*)tree->mgmtData)->fMD.numberOfEntries++;
forceFlushPool(bufferManager); // flush the buffer
//printf("done key insert");
return RC_OK;

}


// delete key
RC deleteKey(BTreeHandle *tree, Value *key) {
    // Access necessary components from the tree structure
    treeData *treeMgmt = (treeData *)tree->mgmtData;
    BM_BufferPool *bufferManager = treeMgmt->bufferManager;
    BM_PageHandle *pageHandler = treeMgmt->pageHandler;

    // Retrieve root page metadata and data
    int rootPageNumber = treeMgmt->fMD.rootPgNumber;
    pageData rootPageData;
    if (readpageData(bufferManager, pageHandler, &rootPageData, rootPageNumber) != RC_OK) {
        return RC_READ_FAILED; // Return an error if the root page data could not be read
    }

    // Find the leaf page that contains the key
    pageData targetPage = locatePageToInsertData(bufferManager, pageHandler, rootPageData, key->v.intV);
    
    // Attempt to remove the key from the target page
    RC deleteStatus = keyAndPointerDeletingInLeaf(&targetPage, key->v.intV);
    if (deleteStatus == RC_IM_KEY_NOT_FOUND) {
        return RC_IM_KEY_NOT_FOUND; // Key not found, return appropriate status
    }

    // Serialize the updated page data into a writable format
    char *pageContent = NULL;
    if (allocateSpace(&pageContent) != RC_OK) {
        return RC_MEMORY_ALLOCATION_FAILED; // Return error if memory allocation fails
    }

    preparePageDataToWrite(&targetPage, pageContent); // Prepare data for writing to the page

    // Write the updated data back to the page and mark it as dirty
    if (pageWrite(bufferManager, pageHandler, pageContent, targetPage.pgNumber) != RC_OK) {
        deallocateSpace(&pageContent);
        return RC_WRITE_FAILED; // Error writing data back to page
    }

    // Clean up allocated resources
    deallocateSpace(&pageContent);

    return RC_OK; // Deletion successful
}


// open tree scan
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
    // Define and allocate memory for the scan handle and scan metadata in one step
    BT_ScanHandle *scanHandle = (BT_ScanHandle *)malloc(sizeof(BT_ScanHandle));
    scanData *scanMtdata = (scanData *)malloc(sizeof(scanData));

    // Check for allocation success
    if (!scanHandle || !scanMtdata) {
        return RC_MEMORY_ALLOCATION_FAILED;
    }

    BM_PageHandle *pageHandler = ((treeData*)tree->mgmtData)->pageHandler;
    BM_BufferPool *bufferManager = ((treeData*)tree->mgmtData)->bufferManager;
    int rootPageNum = ((treeData*)tree->mgmtData)->fMD.rootPgNumber;

    // Read root page data
    pageData rootPg;
    readpageData(bufferManager, pageHandler, &rootPg, rootPageNum);

    // Allocate memory for storing leaf page numbers and initialize the counter
    int *leafPages = (int *)malloc(100 * sizeof(int));
    if (!leafPages) {
        free(scanHandle);
        free(scanMtdata);
        return RC_MEMORY_ALLOCATION_FAILED;
    }

    // Retrieve and initialize leaf pages from root
counter = 0;
getLeafPg(rootPg, bufferManager, pageHandler, leafPages);

// Configure scan metadata
scanMtdata->leafPage = leafPages;
scanMtdata->curPage = leafPages[0];
scanMtdata->nextPagePosInLeafPages = 1;
scanMtdata->curPosInPage = 0;
scanMtdata->isCurPageLoaded = 1;
scanMtdata->noOfLeafPage = counter;

// Load data for the current page
pageData currentLeafPage;
readpageData(bufferManager, pageHandler, &currentLeafPage, scanMtdata->curPage);
scanMtdata->curPageData = currentLeafPage;

// Link scan metadata to scan handle
scanHandle->mgmtData = scanMtdata;
scanHandle->tree = tree;

// Set the scan handle for the caller and return success
*handle = scanHandle;

return RC_OK;

}


//next entry
RC nextEntry(BT_ScanHandle *handle, RID *result) {
    BM_BufferPool *bufferManager = ((treeData*)handle->tree->mgmtData)->bufferManager;
    BM_PageHandle *pageHandler = ((treeData*)handle->tree->mgmtData)->pageHandler; 
    scanData* scanData = handle->mgmtData;

    switch (scanData->curPosInPage >= scanData->curPageData.numberEntries) {
        case 1: // Check if the current position is beyond the number of entries on the current page
            switch (scanData->nextPagePosInLeafPages) {
                case -1:  // No more leaf pages to scan
                    return RC_IM_NO_MORE_ENTRIES;
                default:
                    // Move to the next leaf page
                    scanData->curPage = scanData->leafPage[scanData->nextPagePosInLeafPages];
                    scanData->isCurPageLoaded = 0;
                    scanData->nextPagePosInLeafPages += 1;

                    // Reset the nextPagePosInLeafPages if it exceeds the available number of leaf pages
                    if (scanData->nextPagePosInLeafPages >= scanData->noOfLeafPage) {
                        scanData->nextPagePosInLeafPages = -1;
                    }
                    break;
            }
            break;

        default:  // No need to move to the next leaf page
            break;
    }

    if (!scanData->isCurPageLoaded) {
        pageData leafPg;
        readpageData(bufferManager, pageHandler, &leafPg, scanData->curPage);
        scanData->curPageData = leafPg;
        scanData->curPosInPage = 0;
        scanData->isCurPageLoaded = 1;   
    }

    // Update slot and page
    float c = scanData->curPageData.pointers[scanData->curPosInPage];
    int cValue = round(c * 10);
    result->slot = cValue % 10;
    result->page = cValue / 10; 

    scanData->curPosInPage += 1;
    
    return RC_OK;
}


// close tree scan
RC closeTreeScan(BT_ScanHandle *handle) {
    // Check if the handle is valid
    if (handle == NULL) {
        return RC_ERROR; // Handle invalid input gracefully
    }

    // Clean up any allocated memory in mgmtData if it exists
    if (handle->mgmtData != NULL) {
        memset(handle->mgmtData, 0, sizeof(*(handle->mgmtData))); // Clear the mgmtData
        free(handle->mgmtData);
    }

    // Free the scan handle itself
    free(handle);

    // Set the pointer to NULL to avoid dangling references
    handle = NULL;

    return RC_OK;
}


//**************************************************************************************************