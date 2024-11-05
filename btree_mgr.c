#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "buffer_mgr_stat.h"
#include "dberror.h"
#include "storage_mgr.h"
#include "btree_mgr.h"
#include "buffer_mgr.h"

//************************************Data Structures*************************************

//Data structure about page
typedef struct page_struct_data{
    int leaf;
    int entry_number;
    
    int parent_Node;
    int page_Number;

    float *pointer_to_pages; 
    int *keys; 
    
}page_struct_data;

//Meta data of the file
typedef struct file_Metadata{
    // Root page number of the B+ tree.
    int rootpage_Number;
    int number_of_pageNodes; 
    // Number of entries (key-pointer pairs) per page.
    int entry_Number; 
    int keyType; // optional value
    // Maximum number of entries allowed per page.
    int maxEntriesPerPage;
    
}file_Metadata;

//Mgmt Data
typedef struct tree_DS{

    SM_FileHandle fileHandler;
    file_Metadata fMD;
    BM_PageHandle* pageHandler;
    BM_BufferPool* bufferManager;

}tree_DS;

//Key Data, it has key and left and right pointer_to_pages
typedef struct data{

    float left;
    int key;
    float right;

}data;

//Struct data typer for scanning data
typedef struct scan_tree_data{
    int *leafPage; //entry of leaf pages 
    int cuurent_page;//Page Number of current page
    int current_page_is_loaded;
    page_struct_data cuurent_pageData;
    int nextPagePosInLeafPages;
    int curr_page_position;
    // Total number of leaf pages in the B+ tree.
    int number_of_leaf_pages;
    

}scan_tree_data;

//Global variables
static int counter = 0;       
BT_ScanHandle* scanHandle;
scan_tree_data* scanMetadata;
BTreeHandle* tree_Handle;   
tree_DS* b_Tree_Mgmt;

/************************************************Prototype of helper methods******************************************************/
int parseIntBySeperator(char **ptr, char c);
RC readMetaData(BM_BufferPool* bm,BM_PageHandle* ph,file_Metadata* fmd,int pageNumber);
float parseFloatBySeperator(char **ptr, char c);
RC readPageData(BM_BufferPool* bufferManager, BM_PageHandle* pageHandler, page_struct_data* page_struct_data, int pageNumber);
page_struct_data findLeafPageforInsertion(BM_BufferPool* bufferManager, BM_PageHandle* pageHandler, page_struct_data root, int key);
RC newkeyAndPtrToLeaf(page_struct_data* pageData, int key, RID rid);
RC allocate_Memory(char **data);
RC free_Memory(char **data);
RC formatKeyPointerData(page_struct_data* pd, char* data);
RC formatMetaData(file_Metadata* fmd,char* content);
RC prepareContentWrite(page_struct_data* pd,char* content);
RC writetoBuffer(BM_BufferPool* bm,BM_PageHandle* ph,char* content,int pageNumber);
// Updates parent node pointers to reflect changes in child nodes (like after a split)
RC propagatesplitUp(BTreeHandle *tree,int pageNumber,data kd);
RC updateParentPointer(BTreeHandle* tree,page_struct_data node);
// Inserts a key and its corresponding pointer in a non-leaf page of the B+ tree
RC insertKeyPointer(page_struct_data* page,data kd);
// Deletes a key from a leaf page in the B+ tree.
RC deletekeyInLeaf(page_struct_data* pg, int key);
// Identifies the leaf pages of the B+ tree, starting from the root
RC findLeafPage(page_struct_data root,BM_BufferPool* bm,BM_PageHandle* ph,int* leafPages);

//Initializing the index manager

extern RC initIndexManager (void *mgmtData){
    printf("Initializing Index Manager");
    return RC_OK;
}

extern RC shutdownIndexManager (){
    printf("Shutting down Index manager");
    printf("Shutdown Index Manager performed");
    return RC_OK;
}

//B-Tree index operations including create, edit, delete

//Create brtree
// Function to create a B-tree
RC createBtree (char *idxId, DataType keyType, int n) {

    // Allocate memory for various tree structures
    printf("Allocating memory for tree structures...\n");
    b_Tree_Mgmt = (tree_DS*)malloc(sizeof(tree_DS));
    tree_Handle = (BTreeHandle*)malloc(sizeof(BTreeHandle));
    scanHandle = (BT_ScanHandle*)malloc(sizeof(BT_ScanHandle));
    scanMetadata = (scan_tree_data*)malloc(sizeof(scan_tree_data));

    // Initialize buffer pool, page handler, and scan handler
    printf("Initializing buffer pool and page handler...\n");
    b_Tree_Mgmt->bufferManager = MAKE_POOL();
    b_Tree_Mgmt->pageHandler = MAKE_PAGE_HANDLE();
    tree_Handle->mgmtData = b_Tree_Mgmt;
    scanHandle->mgmtData = scanMetadata;

    // Create a new page file for the B-tree
    printf("Creating page file: %s\n", idxId);
    createPageFile(idxId);

    // Open the newly created page file and link it to the file handler
    printf("Opening page file...\n");
    int result = openPageFile(idxId, &(b_Tree_Mgmt->fileHandler));
    if (result != RC_OK) {
        printf("Error opening page file.\n");
        return result;
    }

    // Initialize B-tree metadata
    printf("Initializing B-tree metadata...\n");
    b_Tree_Mgmt->fMD.number_of_pageNodes = 1;
    b_Tree_Mgmt->fMD.rootpage_Number = 1;
    b_Tree_Mgmt->fMD.entry_Number = 0;
    b_Tree_Mgmt->fMD.maxEntriesPerPage = n;

    // Initialize the buffer pool and ensure a capacity of at least 2 pages
    printf("Initializing buffer pool...\n");
    initBufferPool(b_Tree_Mgmt->bufferManager, idxId, 10, RS_FIFO, NULL);
    printf("Ensuring buffer pool capacity of at least 2 pages...\n");
    ensureCapacity(2, &(b_Tree_Mgmt->fileHandler));

    // Allocate memory for writing metadata to the buffer
    printf("Allocating memory for metadata...\n");
    char *data_str;
    allocate_Memory(&data_str);
    printf("Formatting metadata and writing to buffer...\n");
    formatMetaData(&(b_Tree_Mgmt->fMD), data_str);
    writetoBuffer(b_Tree_Mgmt->bufferManager, b_Tree_Mgmt->pageHandler, data_str, 0);
    free_Memory(&data_str);

    // Allocate memory for initializing the root page
    printf("Setting up root page...\n");
    allocate_Memory(&data_str);
    page_struct_data root;
    root.page_Number = 1;
    root.leaf = 1;            // Indicating it's a leaf node
    root.parent_Node = -1;     // No parent for root
    root.entry_number = 0;     // No entries initially
    
    // Write root page to buffer
    printf("Writing root page to buffer...\n");
    prepareContentWrite(&root, data_str);
    writetoBuffer(b_Tree_Mgmt->bufferManager, b_Tree_Mgmt->pageHandler, data_str, b_Tree_Mgmt->fMD.rootpage_Number);
    free_Memory(&data_str);

    // Shutdown the buffer pool after writing
    printf("Shutting down buffer pool...\n");
    shutdownBufferPool(b_Tree_Mgmt->bufferManager);

    printf("B-tree creation complete.\n");
    
    return RC_OK;
}


// Function to open an existing B-tree
extern RC openBtree (BTreeHandle **tree, char *idxId) {

    // Open the page file for the B-tree
    printf("Opening page file: %s\n", idxId);
    int rt_val = openPageFile(idxId, &(b_Tree_Mgmt->fileHandler)); 

    // Check if the file was successfully opened
    if (rt_val != RC_OK) {
        printf("Failed to open page file. Error code: %d\n", rt_val);
        return rt_val; 
    }
    printf("Page file opened successfully.\n");

    // Initialize the buffer manager and page handler for the B-tree
    printf("Initializing buffer manager and page handler...\n");
    b_Tree_Mgmt->bufferManager = MAKE_POOL();
    b_Tree_Mgmt->pageHandler = MAKE_PAGE_HANDLE();
    initBufferPool(b_Tree_Mgmt->bufferManager, idxId, 10, RS_FIFO, NULL);
    printf("Buffer pool initialized.\n");

    // Read the metadata from the B-tree file
    printf("Reading metadata from B-tree file...\n");
    file_Metadata fmd;
    RC metaReadStatus = readMetaData(b_Tree_Mgmt->bufferManager, b_Tree_Mgmt->pageHandler, &fmd, 0);
    
    if (metaReadStatus != RC_OK) {
        printf("Failed to read metadata. Error code: %d\n", metaReadStatus);
        return metaReadStatus;
    }
    printf("Metadata read successfully.\n");

    // Set up the tree handle and B-tree manager data using the read metadata
    printf("Setting up tree handle and B-tree management data...\n");
    tree_Handle->idxId = idxId;
    tree_Handle->keyType = fmd.keyType;
    b_Tree_Mgmt->fMD.number_of_pageNodes = fmd.number_of_pageNodes;
    b_Tree_Mgmt->fMD.maxEntriesPerPage = fmd.maxEntriesPerPage;
    b_Tree_Mgmt->fMD.rootpage_Number = fmd.rootpage_Number;
    b_Tree_Mgmt->fMD.entry_Number = fmd.entry_Number;

    // Link the management data to the tree handle
    tree_Handle->mgmtData = b_Tree_Mgmt;
    *tree = tree_Handle;

    printf("B-tree opened and initialized successfully.\n");

    return RC_OK;
}

// Function to close the B-tree
extern RC closeBtree (BTreeHandle *tree) {

    // Get the buffer manager from the tree's management data
    printf("Retrieving buffer manager for the B-tree.\n");
    BM_BufferPool *bm = ((tree_DS*)tree->mgmtData)->bufferManager;
    
    // Prepare to write metadata back to disk before closing
    printf("Writing B-tree metadata to disk before closing...\n");
    char *data_str;
    allocate_Memory(&data_str); // Allocate memory for the data
    formatMetaData(&(b_Tree_Mgmt->fMD), data_str); // Format the metadata
    writetoBuffer(b_Tree_Mgmt->bufferManager, b_Tree_Mgmt->pageHandler, data_str, 0); // Write metadata to disk
    free_Memory(&data_str); // Free the memory allocated for the data
    printf("Metadata written to disk successfully.\n");

    // Shutdown the buffer pool to release resources
    printf("Shutting down the buffer pool...\n");
    shutdownBufferPool(bm);
    printf("Buffer pool shutdown complete.\n");

    // Free the allocated memory for the B-tree's data structures
    printf("Freeing allocated memory for B-tree structures...\n");
    free(b_Tree_Mgmt->bufferManager);
    free(b_Tree_Mgmt->pageHandler);
    free(tree->mgmtData);
    free(tree);
    printf("Memory freed successfully. B-tree closed.\n");

    return RC_OK;
}

//Delete tree
extern RC deleteBtree (char *idxId){
    remove(idxId);

    return RC_OK;
}

//************************************Access information about a B-tree*******************

// Get the number of nodes in the B-tree
RC getNumNodes(BTreeHandle *tree, int *result) {
    printf("Fetching the number of nodes in the B-tree...\n");
    *result = ((tree_DS*)tree->mgmtData)->fMD.number_of_pageNodes; // Retrieve the number of page nodes
    printf("Number of nodes: %d\n", *result);
    return RC_OK;
}

// Get the key type used in the B-tree
RC getKeyType(BTreeHandle *tree, DataType *result) {
    printf("Retrieving the key type for the B-tree...\n");
    *result = ((tree_DS*)tree->mgmtData)->fMD.keyType; // Retrieve the key type
    printf("Key type retrieved successfully.\n");
    return RC_OK;
}

// Get the number of entries in the B-tree
RC getNumEntries(BTreeHandle *tree, int *result) {
    printf("Fetching the number of entries in the B-tree...\n");
    *result = ((tree_DS*)tree->mgmtData)->fMD.entry_Number; // Retrieve the number of entries
    printf("Number of entries: %d\n", *result);
    return RC_OK;
}

//***************************************Initializing the Helper functions****************************

int parseIntBySeperator(char **ptr, char c) {
    // Store the current pointer position
    char *tempPtr = *ptr;
    char *val = (char*)malloc(100);

    // Clear memory for 'val'
    memset(val, '\0', 100);

    // Loop through the string until the separator is found
    for (int i = 0; *tempPtr != c; i++, tempPtr++) {
        val[i] = *tempPtr;  // Store each character in 'val'
    }

    // Convert the extracted string into an integer
    int extractedValue = atoi(val);

    // Update the original pointer position
    *ptr = tempPtr;

    // Free the dynamically allocated memory
    free(val);

    // Return the integer value
    return extractedValue;
}

RC readMetaData(BM_BufferPool* bufferManager,BM_PageHandle* pageHandler,file_Metadata* fMD,int page_Number){
    //Read the index metadata from page 0 
    pinPage(bufferManager,pageHandler,page_Number);

    char *cursor = pageHandler->data;

    //printf("I am here");
    cursor++; // Skip initial marker
    // Extract the data between first $ and next $ : root node's page number
    fMD->rootpage_Number = parseIntBySeperator(&cursor,'$');

    //printf("I am done");
    cursor++; // Skip the next section
    fMD->number_of_pageNodes = parseIntBySeperator(&cursor,'$');

    cursor++; // Skip the next section
    fMD->entry_Number = parseIntBySeperator(&cursor,'$');

    cursor++; // Skip the next section
    fMD->maxEntriesPerPage = parseIntBySeperator(&cursor,'$');

    cursor++; // Skip the next section
    fMD->keyType = parseIntBySeperator(&cursor,'$');

    unpinPage(bufferManager,pageHandler);

    return RC_OK;
}


float getDataBySeperatorForFloat(char **ptr, char c){
    char *tempPtr = *ptr;
    char *val=(char*)malloc(100);

    //printf("\nstart float seperator\n");

    memset(val,'\0',sizeof(val));
    for(int i=0;*tempPtr!=c;tempPtr++,i++){
        val[i]=*tempPtr;
    }
    
    //printf("\nloop start!\n");
    // int index=0;
    // while(*tempPtr!=c){
    //     val[index]=*tempPtr;
    //     tempPtr++;
    //     index++;
    // }
    //printf("\nloop done!\n");
    
    *ptr=tempPtr;
    float val2=atof(val);
    free(val);

   // printf("\nend float seperator\n");

    return val2;
}

RC readPageData(BM_BufferPool* bufferManager, BM_PageHandle* pageHandler, page_struct_data* page_struct_data, int pageNumber){
    
    pinPage(bufferManager,pageHandler,pageNumber); // pinning the page

    char *pageHandlerData=pageHandler->data;
    pageHandlerData++; // skip the inital character

    //printf("\nskip initial character\n");

    // reading data
    page_struct_data->leaf =parseIntBySeperator(&pageHandlerData,'$');
    pageHandlerData++;

    page_struct_data->entry_number =parseIntBySeperator(&pageHandlerData,'$');
    pageHandlerData++;
    
    page_struct_data->parent_Node =parseIntBySeperator(&pageHandlerData,'$');
    pageHandlerData++;

    page_struct_data->page_Number =parseIntBySeperator(&pageHandlerData,'$');
    pageHandlerData++;

    //printf("\ndone seperating!\n");

    int index=0;
    float *children=malloc((page_struct_data->entry_number+1)*sizeof(float));
    int *key=malloc(page_struct_data->entry_number*sizeof(int));

    if(page_struct_data->entry_number>0){
        while(index<page_struct_data->entry_number){
            children[index] = getDataBySeperatorForFloat(&pageHandlerData,'$');
            pageHandlerData++;
            key[index] = parseIntBySeperator(&pageHandlerData,'$');
            pageHandlerData++;
            index++;
        }
        children[index]=getDataBySeperatorForFloat(&pageHandlerData,'$');
    }

    //printf("\ndone final seprate!\n");

    page_struct_data->pointer_to_pages=children;
    page_struct_data->keys=key;
    unpinPage(bufferManager,pageHandler);

    return RC_OK;
}

page_struct_data findLeafPageforInsertion(BM_BufferPool* bufferManager, BM_PageHandle* pageHandler, page_struct_data root, int key){
    if(root.leaf){
        return root;
    }
    else{
        if(key<root.keys[0]){
            int pageSearchNumber = round(root.pointer_to_pages[0]*10)/10;
            page_struct_data searchPage;
            readPageData(bufferManager,pageHandler,&searchPage,pageSearchNumber);
            return findLeafPageforInsertion(bufferManager,pageHandler,searchPage,key);
        }
        else{
            int foundPage=0;
            size_t index;
            for(index=0; index < root.entry_number-1;index++){
                if(key>=root.keys[index] && key<root.keys[index+1]){
                    page_struct_data searchInPage;
                    foundPage=1;
                    int pageSearchNumber=round(root.pointer_to_pages[index+1]*10)/10;
                    readPageData(bufferManager,pageHandler,&searchInPage,pageSearchNumber);
                    return findLeafPageforInsertion(bufferManager,pageHandler,searchInPage,key);
                }
                //index++;
            }
            if(!foundPage){
                int pageSearchNumber = round(root.pointer_to_pages[root.entry_number]*10)/10;
                page_struct_data searchPage;
                readPageData(bufferManager,pageHandler,&searchPage,pageSearchNumber);
                return findLeafPageforInsertion(bufferManager,pageHandler,searchPage,key);
            }
        }
    }
}

RC newkeyAndPtrToLeaf(page_struct_data* pageData, int key, RID rid)
{
    float *children=(float*)malloc(sizeof(int)*10);
    int *keys=(int*)malloc(sizeof(int)*10);
    
    int index=0;
    while(index< pageData->entry_number&& key> pageData->keys[index]){
        keys[index]=pageData->keys[index];
        children[index] = pageData->pointer_to_pages[index];
        index++;
    }

    if(index<pageData->entry_number && key == pageData->keys[index]){
        return RC_IM_KEY_ALREADY_EXISTS;
    }
    else{
        float cPointer= rid.page+ rid.slot*0.1;
        keys[index]= key;
        children[index] = cPointer;
        index++;
    }

    while(index < pageData->entry_number+1){
        children[index]=pageData->pointer_to_pages[index-1];
        keys[index]= pageData->keys[index-1];
        index++;
    }

    children[index]=-1;
    free(pageData->keys);
    free(pageData->pointer_to_pages);
    pageData->keys= keys;
    pageData->pointer_to_pages=children;
    pageData->entry_number++;
    return RC_OK;
}

RC allocate_Memory(char **data){
    *data=(char*)malloc(50*sizeof(char));
    memset(*data,'\0',50);
    //printf("space allocated");
}

RC free_Memory(char **data){
    free(*data);
    //printf("space deallocated");
}
RC formatKeyPointerData(page_struct_data* page_struct_data, char* content){
    //printf("key pointer formated data");
    char *cursor = content;
    int index = 0;
    
    while(index < page_struct_data->entry_number) {
        int childValue = round(page_struct_data->pointer_to_pages[index]*10);
        int slot =  childValue % 10;
        int pageNum =  childValue / 10;

        sprintf(cursor,"%d.%d$",pageNum,slot);
        cursor += 4;
        sprintf(cursor,"%d$",page_struct_data->keys[index]);
        if(page_struct_data->keys[index] >=10){
            cursor += 3;
        }
        else{
            cursor += 2;
        }
        index++;
    }
    sprintf(cursor,"%0.1f$",page_struct_data->pointer_to_pages[index]);
    //printf("key pointer formated!");
}

RC formatMetaData(file_Metadata* fMD,char* data){
    //Create metadata for this index tree
    sprintf (data,"$%d$%d$%d$%d$%d$",fMD->rootpage_Number,fMD->number_of_pageNodes,fMD->entry_Number,fMD->maxEntriesPerPage,fMD->keyType);
}


RC prepareContentWrite(page_struct_data* pageData,char* content){
    sprintf (content,"$%d$%d$%d$%d$",pageData->leaf,pageData->entry_number,pageData->parent_Node,pageData->page_Number);
    //printf("number of entries: %d",pd->entry_number);
    if(pageData->entry_number > 0){
        char* keysAndpointer_to_pages;
        allocate_Memory(&keysAndpointer_to_pages);
        formatKeyPointerData(pageData,keysAndpointer_to_pages);
        sprintf (content+strlen(content),"%s",keysAndpointer_to_pages);
        //sprintf(content + offset, "%s", keysAndpointer_to_pages); // Append formatted key-pointer data to content
        free_Memory(&keysAndpointer_to_pages);
    }
    //printf("page prepared");
}

RC writetoBuffer(BM_BufferPool* bufferManager,BM_PageHandle* pageHandler,char* content,int pageNumber){
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

RC propagatesplitUp(BTreeHandle *treeHandler,int pgNumb,data keyData){

    BM_BufferPool *bufferManager = ((tree_DS*)treeHandler->mgmtData)->bufferManager;
    BM_PageHandle *pageHandler = ((tree_DS*)treeHandler->mgmtData)->pageHandler;
    SM_FileHandle fileHandler = ((tree_DS*)treeHandler->mgmtData)->fileHandler;
    
    int maxEntity = ((tree_DS*)treeHandler->mgmtData)->fMD.maxEntriesPerPage;
    int curNumOfNodes = ((tree_DS*)treeHandler->mgmtData)->fMD.number_of_pageNodes;
    
    // if parent page number is -1, create new node
    if(pgNumb != -1){
        page_struct_data newPageToAdd; // page is full or not full, add data in existing page
        readPageData(bufferManager,pageHandler,&newPageToAdd,pgNumb);
        insertKeyPointer(&newPageToAdd,keyData);
        
        if(newPageToAdd.entry_number > maxEntity){ // if page have more than max entries
            // if is true, divide and copy the data into new pages, the key of new node will be in right child and old node will be in left child. 
            float *oldNodeChildren = (float *)malloc(10*sizeof(float));
            int count = 0, *oldNodeKeys = (int *)malloc(10*sizeof(int));
            size_t index = 0;
            while(index < (int)ceil((newPageToAdd.entry_number)/2))
            {
                oldNodeKeys[count] = newPageToAdd.keys[index];
                oldNodeChildren[count] = newPageToAdd.pointer_to_pages[index];
                count++;
                index++;
            }
            oldNodeChildren[count] = newPageToAdd.pointer_to_pages[count];
            count++;

            int count2 = 0,*keysForNewNode = (int *)malloc(10*sizeof(int));
            float *childrenForNewNode = (float *)malloc(10*sizeof(float));
            index=count;
            while(index < newPageToAdd.entry_number + 2)
            {
                keysForNewNode[count2] = newPageToAdd.keys[index];
                childrenForNewNode[count2] = newPageToAdd.pointer_to_pages[index];
                count2++;
                index++;
            }
            childrenForNewNode[count2] = newPageToAdd.pointer_to_pages[count];

            ensureCapacity (curNumOfNodes + 2, &fileHandler);

            curNumOfNodes += 1;

            ((tree_DS*)treeHandler->mgmtData)->fMD.number_of_pageNodes++;

            //Right child data
            page_struct_data pRChild;
            pRChild.leaf = 0;
            pRChild.page_Number = curNumOfNodes;
            pRChild.entry_number = (int)floor((maxEntity+1)/2);
            pRChild.keys = keysForNewNode;
            pRChild.pointer_to_pages = childrenForNewNode;
            pRChild.parent_Node = newPageToAdd.parent_Node;

            //data update on right child
            char *dataStr;
            allocate_Memory(&dataStr);
            prepareContentWrite(&pRChild,dataStr);
            writetoBuffer(bufferManager,pageHandler,dataStr,pRChild.page_Number);
            free_Memory(&dataStr);

            //Left child Data
            page_struct_data pLChild;
            pLChild.leaf = 0;
            pLChild.page_Number = newPageToAdd.page_Number;
            pLChild.entry_number = (int)floor((maxEntity+1)/2);
            pLChild.keys = oldNodeKeys;
            pLChild.pointer_to_pages = oldNodeChildren;
            pLChild.parent_Node = newPageToAdd.parent_Node;

            //Update data on left child
            allocate_Memory(&dataStr);
            prepareContentWrite(&pLChild,dataStr);
            writetoBuffer(bufferManager,pageHandler,dataStr,pLChild.page_Number);
            free_Memory(&dataStr);

            int pgNum = newPageToAdd.parent_Node;
            float left = pLChild.page_Number;
            float right = pRChild.page_Number;

            data kdata;
            kdata.key = newPageToAdd.keys[(int)ceil((maxEntity+1)/2)];
            kdata.left = left;
            kdata.right = right;

            //update childe of each parent
            updateParentPointer(treeHandler,pRChild);

            //propagate up
            propagatesplitUp(treeHandler,pgNum,kdata);

        }
        else{
            char *dataString;
            allocate_Memory(&dataString);
            prepareContentWrite(&newPageToAdd,dataString);
            writetoBuffer(bufferManager,pageHandler,dataString,newPageToAdd.page_Number);
            free_Memory(&dataString);
            return RC_OK;   
        }

    }
    else{
        int curNumOfNodes = ((tree_DS*)treeHandler->mgmtData)->fMD.number_of_pageNodes;
        ensureCapacity(curNumOfNodes+2,&fileHandler);

        page_struct_data newRoot; // making new node as root
        newRoot.page_Number = curNumOfNodes+1;

        int *keysofNewRoot = (int *)malloc(10*sizeof(int));
        keysofNewRoot[0] = keyData.key;

        float *childrenofNewRoot = (float *)malloc(10*sizeof(float));
        childrenofNewRoot[0] = keyData.left;
        childrenofNewRoot[1] = keyData.right;

        newRoot.keys = keysofNewRoot;
        newRoot.pointer_to_pages= childrenofNewRoot;
        newRoot.entry_number = 1;
        newRoot.parent_Node = -1;
        newRoot.leaf = 0;

        ((tree_DS*)treeHandler->mgmtData)->fMD.number_of_pageNodes++;
        ((tree_DS*)treeHandler->mgmtData)->fMD.rootpage_Number = newRoot.page_Number;

        char *dataString;
        allocate_Memory(&dataString);
        prepareContentWrite(&newRoot,dataString);
        writetoBuffer(bufferManager,pageHandler,dataString,newRoot.page_Number);
        free_Memory(&dataString);

        //updating child nodes of each parent
        updateParentPointer(treeHandler,newRoot);
    }
}

RC updateParentPointer(BTreeHandle* tree,page_struct_data dataNode){

    BM_BufferPool *bufferManager = ((tree_DS*)tree->mgmtData)->bufferManager;
    BM_PageHandle *pageHandler = ((tree_DS*)tree->mgmtData)->pageHandler;
    
    // int numChildren = node.entry_number+1;
    size_t index = 0;

    while(index < dataNode.entry_number+1) {
        page_struct_data child;
        readPageData(bufferManager,pageHandler,&child,dataNode.pointer_to_pages[index]);

        //Update the parent
        child.parent_Node = dataNode.page_Number;

        char *data;
        allocate_Memory(&data);
        prepareContentWrite(&child,data);
        writetoBuffer(bufferManager,pageHandler,data,child.page_Number);
        free_Memory(&data);
        index++;
    }

    return RC_OK;

};



RC insertKeyPointer(page_struct_data* page_struct_data,data keyData){
    int *updatedKeys = (int*)malloc(sizeof(int)*10); // Array for new keys
    float *updatedpointer_to_pages = (float*)malloc(sizeof(int)*10); // Array for new pointer_to_pages
    int currentPosition = 0;
    while(keyData.key > page_struct_data->keys[currentPosition] && currentPosition < page_struct_data->entry_number){
        updatedKeys[currentPosition] = page_struct_data->keys[currentPosition];
        updatedpointer_to_pages[currentPosition] = page_struct_data->pointer_to_pages[currentPosition];
        currentPosition++;
    }

    if(keyData.key == page_struct_data->keys[currentPosition] && currentPosition < page_struct_data->entry_number){
        return RC_IM_KEY_ALREADY_EXISTS;
    }
    else{
        updatedKeys[currentPosition] = keyData.key;
        updatedpointer_to_pages[currentPosition] = keyData.left;
        updatedpointer_to_pages[currentPosition+1] = keyData.right;
        currentPosition++;
        updatedKeys[currentPosition] = page_struct_data->keys[currentPosition-1];
        currentPosition++;
        
    }

    // for (int j = currentPosition; j < page->entry_number + 2; j++) {
    // updatedKeys[j] = page->keys[j - 1];
    // updatedpointer_to_pages[j] = page->pointer_to_pages[j - 1];
    // }
    while(currentPosition<page_struct_data->entry_number+2){
        updatedKeys[currentPosition]=page_struct_data->keys[currentPosition-1];
        updatedpointer_to_pages[currentPosition]=page_struct_data->pointer_to_pages[currentPosition-1];
        currentPosition++;
    }

    updatedpointer_to_pages[currentPosition] = -1;
    free(page_struct_data->keys);
    free(page_struct_data->pointer_to_pages);
    page_struct_data->entry_number++;
    page_struct_data->pointer_to_pages = updatedpointer_to_pages;
    page_struct_data->keys = updatedKeys;
   
    return RC_OK;
}

RC deletekeyInLeaf(page_struct_data* pageData, int key){
    int keys[5] = {0},count = 0;
    float children[5] = {0};
    bool found = false;
    for(int i = 0;i < pageData->entry_number;i++){
        if(key == pageData->keys[i] && i < pageData->entry_number){
            //i++;
            found = true;
            continue;
        }
        keys[count] = pageData->keys[i];
        children[count] = pageData->pointer_to_pages[i];
        //i++;
        count++;
    }
    if(!found){
        return RC_IM_KEY_NOT_FOUND;
    }
    //free(page->keys);
    //free(page->children);
    pageData->entry_number -= 1;
    children[count] = -1;
    size_t index = 0;
    while(index < count)
    {
        pageData->keys[index] = keys[index];
        pageData->pointer_to_pages[index] = children[index];
        index++;
    }
    pageData->pointer_to_pages[count] = children[count];
    return RC_OK;
}

RC findLeafPage(page_struct_data page,BM_BufferPool* bufferManager,BM_PageHandle* pageHandler,int* lPages){   
    
    if(!page.leaf){
        size_t childIndex = 0;
        while(childIndex<page.entry_number+1)
        {
            page_struct_data child;
            readPageData(bufferManager,pageHandler,&child,(int)page.pointer_to_pages[childIndex]);
            if(findLeafPage(child,bufferManager,pageHandler,lPages) == RC_OK){
                childIndex++;
                continue;
            }
            childIndex++;
        }
    }
    else{
        lPages[counter] = page.page_Number;
        counter += 1;
        return RC_OK;
    }
    return RC_OK;
}

//****************************************************************************************


//********************************Index Acess functions***********************************

// to find the given key
RC findKey(BTreeHandle *tree, Value *key, RID *result){
    
    // loading the main into the buffer
    SM_FileHandle fileHandler= ((tree_DS*)tree->mgmtData)->fileHandler;
    BM_PageHandle *pageHander = ((tree_DS*)tree->mgmtData)->pageHandler;
    BM_BufferPool *buffermanager= ((tree_DS*)tree->mgmtData)->bufferManager;

    page_struct_data rootpage_struct_data;// root page data

    // getting the root page number
    int rootpage_Number=((tree_DS*)tree->mgmtData)->fMD.rootpage_Number;
    readPageData(buffermanager,pageHander,&rootpage_struct_data,rootpage_Number);

    page_struct_data leafPageData= findLeafPageforInsertion(buffermanager,pageHander,rootpage_struct_data,key->v.intV);

    size_t index=0;

    while(index<leafPageData.entry_number){
        if(leafPageData.keys[index] == key->v.intV){ // checking for the key
            
            float c=leafPageData.pointer_to_pages[index];
            int cValue =round(c*10);
            int page_Number= cValue /10;
            int slot=cValue%10;

            // updating slot and page number if key is found
            result->slot=slot;
            result->page=page_Number;

            return RC_OK;
        }
        index++;
    }

    return RC_IM_KEY_NOT_FOUND;
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid){
    
    //printf("\nstart insert key\n");

    // getting the page handler, buffer manager and file handler
    BM_PageHandle *pageHandler = ((tree_DS*)tree->mgmtData)->pageHandler;
    BM_BufferPool *bufferManager = ((tree_DS*)tree->mgmtData)->bufferManager;
    SM_FileHandle fileHandler = ((tree_DS*)tree->mgmtData)->fileHandler;

    int maxEntry = ((tree_DS*)tree->mgmtData)->fMD.maxEntriesPerPage;
    int curNumOfNode = ((tree_DS*)tree->mgmtData)->fMD.number_of_pageNodes;
    int rootPgNum = ((tree_DS*)tree->mgmtData)->fMD.rootpage_Number;
    
    page_struct_data rootPage; // getting the root page
    readPageData(bufferManager,pageHandler,&rootPage,rootPgNum);
    //printf("read pg data");

    page_struct_data insertionPage; // getting the page where data can be inserted
    insertionPage = findLeafPageforInsertion(bufferManager,pageHandler,rootPage,key->v.intV);

    //printf("\nlocated page!------------\n");
    
    if(newkeyAndPtrToLeaf(&insertionPage,key->v.intV,rid) == RC_IM_KEY_ALREADY_EXISTS){
        return RC_IM_KEY_ALREADY_EXISTS;
    }
    
    //printf("\nnew key and ptr to leaf!------------\n");

    if(insertionPage.entry_number > maxEntry){ // check if there is no space in the leaf node
        
        // creating new nodes
        int counter = 0;
        float *newNodechildren = (float *)malloc(10*sizeof(float));
        int *newNodeKeys = (int *)malloc(10*sizeof(int));
        
        for (size_t i = (int)ceil((insertionPage.entry_number)/2)+1; i < insertionPage.entry_number; i++)
        {
            newNodechildren[counter] = insertionPage.pointer_to_pages[i];
            newNodeKeys[counter] = insertionPage.keys[i];
            counter++;
        }
        newNodechildren[counter] = -1;

        
        counter = 0;
        int *oldNodeKeys = (int *)malloc(10*sizeof(int));
        float *oldNodeChildren = (float *)malloc(10*sizeof(float));

        for (size_t i = 0; i <= (int)ceil((insertionPage.entry_number)/2); i++)
        {
            oldNodeKeys[counter] = insertionPage.keys[i];
            oldNodeChildren[counter] = insertionPage.pointer_to_pages[i];
            counter++;
        }
        oldNodeChildren[counter] = -1;

        ensureCapacity (curNumOfNode + 2, &fileHandler);

        curNumOfNode += 1;

        ((tree_DS*)tree->mgmtData)->fMD.number_of_pageNodes++;

        // setting data for right child
        page_struct_data rightChild;
        rightChild.page_Number = curNumOfNode;
        rightChild.leaf = 1;
        rightChild.pointer_to_pages = newNodechildren;
        rightChild.entry_number = (int)floor((maxEntry+1)/2);
        rightChild.keys = newNodeKeys;

        if(insertionPage.parent_Node == -1)rightChild.parent_Node = 3;
        else rightChild.parent_Node = insertionPage.parent_Node;

        //printf("start update right child");
        
        // updating data of right child
        char *dataHolder;
        allocate_Memory(&dataHolder);
        prepareContentWrite(&rightChild,dataHolder);
        writetoBuffer(bufferManager,pageHandler,dataHolder,rightChild.page_Number);
        free_Memory(&dataHolder);
        //printf("updated right child");


        // left child data
        page_struct_data leftChild;
        leftChild.leaf = 1;
        leftChild.page_Number = insertionPage.page_Number;
        leftChild.entry_number = (int)ceil((maxEntry+1)/2)+1;
        leftChild.keys = oldNodeKeys;
        leftChild.pointer_to_pages = oldNodeChildren;
    
        if(insertionPage.parent_Node == -1) leftChild.parent_Node = 3;
        else leftChild.parent_Node = insertionPage.parent_Node;
        
        //printf("start update left child");
        
        // updating left child data
        allocate_Memory(&dataHolder);
        prepareContentWrite(&leftChild,dataHolder);
        writetoBuffer(bufferManager,pageHandler,dataHolder,leftChild.page_Number);
        free_Memory(&dataHolder);
        //printf("updated left child");

        int page_Number = insertionPage.parent_Node;
        float left = leftChild.page_Number, right = rightChild.page_Number;

        data keyData;
        keyData.left = left;
        keyData.key = rightChild.keys[0];
        keyData.right = right;

        //printf("progate start");
        propagatesplitUp(tree,page_Number,keyData); // propagate up
        //printf("progate end");
    }
    else{
        char *dataHolder = malloc(500);
        //printf("at root--------");
        allocate_Memory(&dataHolder);
        prepareContentWrite(&insertionPage,dataHolder);
        writetoBuffer(bufferManager,pageHandler,dataHolder,insertionPage.page_Number);
        free_Memory(&dataHolder);
        //printf("done root");
    }

    ((tree_DS*)tree->mgmtData)->fMD.entry_Number++; // change the number of entries

    forceFlushPool(bufferManager); // flush the buffer
    //printf("done key insert");
    return RC_OK;
    
}

// delete key
RC deleteKey (BTreeHandle *tree, Value *key){
    
    // getting buffer manager and page handler
    BM_BufferPool *bufferManager = ((tree_DS*)tree->mgmtData)->bufferManager;
    BM_PageHandle *pageHandler = ((tree_DS*)tree->mgmtData)->pageHandler;
    
    page_struct_data rootPg; // getting the root page
    int rootPgIndex = ((tree_DS*)tree->mgmtData)->fMD.rootpage_Number;
    readPageData(bufferManager,pageHandler,&rootPg,rootPgIndex);

    page_struct_data pageData  = findLeafPageforInsertion(bufferManager,pageHandler,rootPg,key->v.intV);

    if(deletekeyInLeaf(&pageData,key->v.intV) == RC_IM_KEY_NOT_FOUND) // deleting the key
        return RC_IM_KEY_NOT_FOUND; // if key not found

    char *updatedData;
    
    // updating the data
    allocate_Memory(&updatedData);
    prepareContentWrite(&pageData,updatedData);
    writetoBuffer(bufferManager,pageHandler,updatedData,pageData.page_Number);
    free_Memory(&updatedData);

    return RC_OK;
}

// open tree scan
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle){
    
    BM_PageHandle *pageHandler = ((tree_DS*)tree->mgmtData)->pageHandler;

    BM_BufferPool *bufferManager = ((tree_DS*)tree->mgmtData)->bufferManager;
    
    // allocating space for scan handler and scan manager
    scanMetadata = (scan_tree_data*)malloc(sizeof(scan_tree_data));
    scanHandle = (BT_ScanHandle*)malloc(sizeof(BT_ScanHandle));
    
    int rootPageNum = ((tree_DS*)tree->mgmtData)->fMD.rootpage_Number;

    page_struct_data rootPg;
    readPageData(bufferManager,pageHandler,&rootPg,rootPageNum);
    
   // Allocate memory for storing leaf page numbers
    int *leafPages = (int *)malloc(100*sizeof(int));
    counter = 0;
    findLeafPage(rootPg,bufferManager,pageHandler,leafPages);
    //printf("\nInside tree scan!\n");
    scanMetadata->leafPage = leafPages;
    scanMetadata->cuurent_page = leafPages[0];    
    page_struct_data leafPage;
    readPageData(bufferManager,pageHandler,&leafPage,scanMetadata->cuurent_page);
    scanMetadata->cuurent_pageData = leafPage;
    scanMetadata->nextPagePosInLeafPages = 1;
    scanMetadata->curr_page_position = 0;
    scanMetadata->number_of_leaf_pages = counter;
    scanMetadata->current_page_is_loaded = 1;

    scanHandle->mgmtData = scanMetadata;
    scanHandle->tree = tree;
    *handle = scanHandle;

    return RC_OK;
}

//next entry
RC nextEntry (BT_ScanHandle *handle, RID *result){
    
    BM_BufferPool *bufferManager = ((tree_DS*)handle->tree->mgmtData)->bufferManager;
    BM_PageHandle *pageHandler = ((tree_DS*)handle->tree->mgmtData)->pageHandler; 
    scan_tree_data* scan_tree_data = handle->mgmtData;

    if(scan_tree_data->curr_page_position >= scan_tree_data->cuurent_pageData.entry_number){ // Check if the current position is beyond the number of entries on the current page
        // Check if there are no leaf pages to scan
        if(scan_tree_data->nextPagePosInLeafPages == -1 ){
            return RC_IM_NO_MORE_ENTRIES;
        }
        // Move to the next leaf page
        scan_tree_data->cuurent_page = scan_tree_data->leafPage[scan_tree_data->nextPagePosInLeafPages];
        scan_tree_data->current_page_is_loaded = 0;
        scan_tree_data->nextPagePosInLeafPages += 1;

        if(scan_tree_data->nextPagePosInLeafPages >= scan_tree_data->number_of_leaf_pages){
            scan_tree_data->nextPagePosInLeafPages = -1;
        }
    }

    if(!scan_tree_data->current_page_is_loaded){
        page_struct_data leafPg;
        readPageData(bufferManager,pageHandler,&leafPg,scan_tree_data->cuurent_page);
        scan_tree_data->cuurent_pageData = leafPg;
        scan_tree_data->curr_page_position = 0;
        scan_tree_data->current_page_is_loaded = 1;   
    }

    // updating slot and page
    float c = scan_tree_data->cuurent_pageData.pointer_to_pages[scan_tree_data->curr_page_position];
    int cValue = round(c*10);
    result->slot =  cValue % 10;
    result->page =  cValue / 10; 
    scan_tree_data->curr_page_position += 1;
    
    return RC_OK;
}

// close tree scan
RC closeTreeScan (BT_ScanHandle *handle){
    free(handle->mgmtData);
    free(handle);
    handle = NULL;
    return RC_OK;
}

//**************************************************************************************************