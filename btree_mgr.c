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

//Page Data
typedef struct pgData{
    
    // Meta Data
    int numberEntries;
    int leaf;
    int parentnode;
    int pgNumber;

    float *pointers; //n+1 pointers for all pages
    int *keys; //n entries for all pages 
    
}pgData;

//Scan Management Data
typedef struct scanData{

    pgData curPageData;
    int nextPagePosInLeafPages;
    int curPosInPage;
    int noOfLeafPage;
    int *leafPage; //entry of leaf pages 
    int curPage;//Page Number of current page
    int isCurPageLoaded;

}scanData;

//Key Data, it has key and left and right pointers
typedef struct data{

    float left;
    int key;
    float right;

}data;

//Meta data of the file
typedef struct fileMD{
    
    int rootPgNumber;
    int numberOfNodes; // number of page nodes
    int numberOfEntries; //Total number of entries in the file
    int keyType; // optional value
    int maxEntriesPerPage;
    
}fileMD;

//Mgmt Data
typedef struct treeData{

    SM_FileHandle fileHandler;
    fileMD fMD;
    BM_PageHandle* pageHandler;
    BM_BufferPool* bufferManager;

}treeData;

//Global variables
static int counter = 0;       
BT_ScanHandle* scanHandle;
scanData* scanMtdata;
BTreeHandle* trHandle;   
treeData* btreeMt;

//****************************************************************************************


//************************************Helper method prototype*****************************

int getDataBySeperatorForInt(char **ptr, char c);
RC readMetaData(BM_BufferPool* bm,BM_PageHandle* ph,fileMD* fmd,int pageNumber);
float getDataBySeperatorForFloat(char **ptr, char c);
RC readPgData(BM_BufferPool* bufferManager, BM_PageHandle* pageHandler, pgData* pgData, int pageNumber);
pgData locatePageToInsertData(BM_BufferPool* bufferManager, BM_PageHandle* pageHandler, pgData root, int key);
RC newkeyAndPtrToLeaf(pgData* pageData, int key, RID rid);
RC allocateSpace(char **data);
RC deallocateSpace(char **data);
RC formatDataofkeyandPtr(pgData* pd, char* data);
RC prepareMetaData(fileMD* fmd,char* content);
RC preparePageDataToWrite(pgData* pd,char* content);
RC pageWrite(BM_BufferPool* bm,BM_PageHandle* ph,char* content,int pageNumber);
RC propagateUp(BTreeHandle *tree,int pageNumber,data kd);
RC updateChildNodesOfParentDown(BTreeHandle* tree,pgData node);
RC insertKeyAndPointerInNonLeaf(pgData* page,data kd);
RC keyAndPointerDeletingInLeaf(pgData* pg, int key);
RC getLeafPg(pgData root,BM_BufferPool* bm,BM_PageHandle* ph,int* leafPages);


//****************************************************************************************


//************************************Initiate and Shutdown*******************************

extern RC initIndexManager (void *mgmtData){
    printf("*******Index Manager Intialized:*******");
    return RC_OK;
}

extern RC shutdownIndexManager (){
    return RC_OK;
}

//****************************************************************************************


//************************************create, destroy, open, and close an btree index*****

//Create brtree
RC createBtree (char *idxId, DataType keyType, int n) {
    //printf("create b tree");

    btreeMt = ( treeData*)malloc (sizeof(treeData) );
    trHandle = ( BTreeHandle*)malloc (sizeof(BTreeHandle) );
    scanHandle = ( BT_ScanHandle*)malloc (sizeof(BT_ScanHandle) );
    scanMtdata = ( scanData*)malloc (sizeof(scanData) );
    
    // initalising buffer pool, page handler and scan handler
    btreeMt -> bufferManager = MAKE_POOL();
    btreeMt -> pageHandler = MAKE_PAGE_HANDLE();
    trHandle -> mgmtData = btreeMt;
    scanHandle -> mgmtData = scanMtdata;

    //scanMtdata = ( scanData*)malloc (sizeof(scanData) ); 
    createPageFile(idxId); // creating page file

    int rt_val = openPageFile(idxId, &(btreeMt->fileHandler));
    //printf("page file created");
    
    // setting the intital btree metadata
    btreeMt ->fMD.numberOfNodes = 1;
    btreeMt ->fMD.rootPgNumber = 1;
    btreeMt ->fMD.numberOfEntries = 0;
    btreeMt ->fMD.maxEntriesPerPage = n;

    initBufferPool(btreeMt -> bufferManager, idxId, 10, RS_FIFO, NULL);
    ensureCapacity(2,&(btreeMt -> fileHandler)); // to ensure that 2 pages are present

    char *data_str;

    //printf("buffer pool initalized ");
    allocateSpace( &data_str );
    prepareMetaData( &(btreeMt -> fMD ), data_str );
    pageWrite( btreeMt -> bufferManager, btreeMt -> pageHandler, data_str, 0 );
    deallocateSpace(&data_str);

    //printf("page written");

    allocateSpace( &data_str );
    pgData root;

    root.pgNumber = 1;
    root.leaf = 1;
    root.parentnode = -1;
    root.numberEntries = 0;    

    allocateSpace( &data_str );
    preparePageDataToWrite( &root, data_str );
    pageWrite( btreeMt -> bufferManager, btreeMt -> pageHandler, data_str, btreeMt ->fMD.rootPgNumber );
    //printf("page writted!");
    deallocateSpace( &data_str );
    
    shutdownBufferPool(btreeMt->bufferManager); // shutdown the buffer pool
    //printf("b tree created");
    
    return RC_OK;
}

//Open btree
extern RC openBtree (BTreeHandle **tree, char *idxId){
    int rt_val = openPageFile( idxId, &(btreeMt -> fileHandler) ); // open the page file
    
    //printf("file opened");

    if (rt_val != RC_OK) { // check if file is opened
        return rt_val; 
    }

    // intialise the buffer manager and page handler
    btreeMt -> bufferManager = MAKE_POOL();
    btreeMt -> pageHandler = MAKE_PAGE_HANDLE();
    initBufferPool( btreeMt -> bufferManager, idxId,10, RS_FIFO,NULL );

    //printf("buffer pool initialized");

    // reading meta data
    fileMD fmd;
    readMetaData( btreeMt -> bufferManager, btreeMt -> pageHandler, &fmd,0);

    //printf("read meta data");

    // adding the data to tree handler and b tree manager
    trHandle -> idxId = idxId;
    trHandle -> keyType = fmd.keyType;
    btreeMt -> fMD.numberOfNodes = fmd.numberOfNodes;
    btreeMt -> fMD.maxEntriesPerPage = fmd.maxEntriesPerPage;
    btreeMt -> fMD.rootPgNumber = fmd.rootPgNumber;
    btreeMt -> fMD.numberOfEntries = fmd.numberOfEntries;

    trHandle -> mgmtData = btreeMt;
    *tree = trHandle;

    return RC_OK;
}

//Close
extern RC closeBtree (BTreeHandle *tree){
    // getting the buffer manager
    BM_BufferPool *bm = ((treeData*)tree->mgmtData)->bufferManager;
    // BM_PageHandle *ph = ((treeData*)tree->mgmtData)-> pageHandler;
    // SM_FileHandle fh = ((treeData*)tree->mgmtData)->fileHandler;

    char *data_str;

    // wrting the data into disk before closing the buffer
    allocateSpace( &data_str );
    prepareMetaData( &(btreeMt->fMD), data_str );
    pageWrite( btreeMt -> bufferManager, btreeMt -> pageHandler, data_str, 0 );
    deallocateSpace( &data_str );
    shutdownBufferPool( bm );
    
    // freeing the space
    free( btreeMt->bufferManager );
    free( btreeMt->pageHandler );
    free( tree->mgmtData );
    free(tree);
    //free( trHandle );

    return RC_OK;
}

//Delete tree
extern RC deleteBtree (char *idxId){
    remove(idxId);

    return RC_OK;
}

//****************************************************************************************


//************************************Access information about a b-tree*******************

RC getNumNodes (BTreeHandle *tree, int *result){ // read the num of nodes
    *result = ((treeData*)tree->mgmtData) -> fMD.numberOfNodes;
    return RC_OK;
}

RC getKeyType(BTreeHandle *tree, DataType *result){ // get the key type
    *result=((treeData*)tree->mgmtData)->fMD.keyType;
    return RC_OK;
}

RC getNumEntries(BTreeHandle *tree, int *result){ // get number of entries
    *result=((treeData*)tree->mgmtData)->fMD.numberOfEntries;
    return RC_OK;
}

//****************************************************************************************

//***************************************Helper functions****************************

int getDataBySeperatorForInt(char **ptr, char c){
    char *tempPtr=*ptr;
    char *val=(char*)malloc(100);
    //printf("\nstart int seperator\n");

    memset(val,'\0',sizeof(val));

    for(int i=0;*tempPtr!=c;i++,tempPtr++){
        val[i]=*tempPtr;
    }

    int val2=atoi(val);
    *ptr=tempPtr;
    free(val);
    
    //printf("\nend int seperator\n");

    return val2;
    
}

RC readMetaData(BM_BufferPool* bufferManager,BM_PageHandle* pageHandler,fileMD* fMD,int pgNumber){
    //Read the index metadata from page 0 
    pinPage(bufferManager,pageHandler,pgNumber);

    char *cursor = pageHandler->data;

    //printf("I am here");
    cursor++; // Skip initial marker
    // Extract the data between first $ and next $ : root node's page number
    fMD->rootPgNumber = getDataBySeperatorForInt(&cursor,'$');

    //printf("I am done");
    cursor++; // Skip the next section
    fMD->numberOfNodes = getDataBySeperatorForInt(&cursor,'$');

    cursor++; // Skip the next section
    fMD->numberOfEntries = getDataBySeperatorForInt(&cursor,'$');

    cursor++; // Skip the next section
    fMD->maxEntriesPerPage = getDataBySeperatorForInt(&cursor,'$');

    cursor++; // Skip the next section
    fMD->keyType = getDataBySeperatorForInt(&cursor,'$');

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

RC readPgData(BM_BufferPool* bufferManager, BM_PageHandle* pageHandler, pgData* pgData, int pageNumber){
    
    pinPage(bufferManager,pageHandler,pageNumber); // pinning the page

    char *pageHandlerData=pageHandler->data;
    pageHandlerData++; // skip the inital character

    //printf("\nskip initial character\n");

    // reading data
    pgData->leaf =getDataBySeperatorForInt(&pageHandlerData,'$');
    pageHandlerData++;

    pgData->numberEntries =getDataBySeperatorForInt(&pageHandlerData,'$');
    pageHandlerData++;
    
    pgData->parentnode =getDataBySeperatorForInt(&pageHandlerData,'$');
    pageHandlerData++;

    pgData->pgNumber =getDataBySeperatorForInt(&pageHandlerData,'$');
    pageHandlerData++;

    //printf("\ndone seperating!\n");

    int index=0;
    float *children=malloc((pgData->numberEntries+1)*sizeof(float));
    int *key=malloc(pgData->numberEntries*sizeof(int));

    if(pgData->numberEntries>0){
        while(index<pgData->numberEntries){
            children[index] = getDataBySeperatorForFloat(&pageHandlerData,'$');
            pageHandlerData++;
            key[index] = getDataBySeperatorForInt(&pageHandlerData,'$');
            pageHandlerData++;
            index++;
        }
        children[index]=getDataBySeperatorForFloat(&pageHandlerData,'$');
    }

    //printf("\ndone final seprate!\n");

    pgData->pointers=children;
    pgData->keys=key;
    unpinPage(bufferManager,pageHandler);

    return RC_OK;
}

pgData locatePageToInsertData(BM_BufferPool* bufferManager, BM_PageHandle* pageHandler, pgData root, int key){
    if(root.leaf){
        return root;
    }
    else{
        if(key<root.keys[0]){
            int pageSearchNumber = round(root.pointers[0]*10)/10;
            pgData searchPage;
            readPgData(bufferManager,pageHandler,&searchPage,pageSearchNumber);
            return locatePageToInsertData(bufferManager,pageHandler,searchPage,key);
        }
        else{
            int foundPage=0;
            size_t index;
            for(index=0; index < root.numberEntries-1;index++){
                if(key>=root.keys[index] && key<root.keys[index+1]){
                    pgData searchInPage;
                    foundPage=1;
                    int pageSearchNumber=round(root.pointers[index+1]*10)/10;
                    readPgData(bufferManager,pageHandler,&searchInPage,pageSearchNumber);
                    return locatePageToInsertData(bufferManager,pageHandler,searchInPage,key);
                }
                //index++;
            }
            if(!foundPage){
                int pageSearchNumber = round(root.pointers[root.numberEntries]*10)/10;
                pgData searchPage;
                readPgData(bufferManager,pageHandler,&searchPage,pageSearchNumber);
                return locatePageToInsertData(bufferManager,pageHandler,searchPage,key);
            }
        }
    }
}

RC newkeyAndPtrToLeaf(pgData* pageData, int key, RID rid)
{
    float *children=(float*)malloc(sizeof(int)*10);
    int *keys=(int*)malloc(sizeof(int)*10);
    
    int index=0;
    while(index< pageData->numberEntries&& key> pageData->keys[index]){
        keys[index]=pageData->keys[index];
        children[index] = pageData->pointers[index];
        index++;
    }

    if(index<pageData->numberEntries && key == pageData->keys[index]){
        return RC_IM_KEY_ALREADY_EXISTS;
    }
    else{
        float cPointer= rid.page+ rid.slot*0.1;
        keys[index]= key;
        children[index] = cPointer;
        index++;
    }

    while(index < pageData->numberEntries+1){
        children[index]=pageData->pointers[index-1];
        keys[index]= pageData->keys[index-1];
        index++;
    }

    children[index]=-1;
    free(pageData->keys);
    free(pageData->pointers);
    pageData->keys= keys;
    pageData->pointers=children;
    pageData->numberEntries++;
    return RC_OK;
}

RC allocateSpace(char **data){
    *data=(char*)malloc(50*sizeof(char));
    memset(*data,'\0',50);
    //printf("space allocated");
}

RC deallocateSpace(char **data){
    free(*data);
    //printf("space deallocated");
}
RC formatDataofkeyandPtr(pgData* pgData, char* content){
    //printf("key pointer formated data");
    char *cursor = content;
    int index = 0;
    
    while(index < pgData->numberEntries) {
        int childValue = round(pgData->pointers[index]*10);
        int slot =  childValue % 10;
        int pageNum =  childValue / 10;

        sprintf(cursor,"%d.%d$",pageNum,slot);
        cursor += 4;
        sprintf(cursor,"%d$",pgData->keys[index]);
        if(pgData->keys[index] >=10){
            cursor += 3;
        }
        else{
            cursor += 2;
        }
        index++;
    }
    sprintf(cursor,"%0.1f$",pgData->pointers[index]);
    //printf("key pointer formated!");
}

RC prepareMetaData(fileMD* fMD,char* data){
    //Create metadata for this index tree
    sprintf (data,"$%d$%d$%d$%d$%d$",fMD->rootPgNumber,fMD->numberOfNodes,fMD->numberOfEntries,fMD->maxEntriesPerPage,fMD->keyType);
}


RC preparePageDataToWrite(pgData* pageData,char* content){
    sprintf (content,"$%d$%d$%d$%d$",pageData->leaf,pageData->numberEntries,pageData->parentnode,pageData->pgNumber);
    //printf("number of entries: %d",pd->numberEntries);
    if(pageData->numberEntries > 0){
        char* keysAndPointers;
        allocateSpace(&keysAndPointers);
        formatDataofkeyandPtr(pageData,keysAndPointers);
        sprintf (content+strlen(content),"%s",keysAndPointers);
        //sprintf(content + offset, "%s", keysAndPointers); // Append formatted key-pointer data to content
        deallocateSpace(&keysAndPointers);
    }
    //printf("page prepared");
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
        pgData newPageToAdd; // page is full or not full, add data in existing page
        readPgData(bufferManager,pageHandler,&newPageToAdd,pgNumb);
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
            pgData pRChild;
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
            pgData pLChild;
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

        pgData newRoot; // making new node as root
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

RC updateChildNodesOfParentDown(BTreeHandle* tree,pgData dataNode){

    BM_BufferPool *bufferManager = ((treeData*)tree->mgmtData)->bufferManager;
    BM_PageHandle *pageHandler = ((treeData*)tree->mgmtData)->pageHandler;
    
    // int numChildren = node.numberEntries+1;
    size_t index = 0;

    while(index < dataNode.numberEntries+1) {
        pgData child;
        readPgData(bufferManager,pageHandler,&child,dataNode.pointers[index]);

        //Update the parent
        child.parentnode = dataNode.pgNumber;

        char *data;
        allocateSpace(&data);
        preparePageDataToWrite(&child,data);
        pageWrite(bufferManager,pageHandler,data,child.pgNumber);
        deallocateSpace(&data);
        index++;
    }

    return RC_OK;

};



RC insertKeyAndPointerInNonLeaf(pgData* pgData,data keyData){
    int *updatedKeys = (int*)malloc(sizeof(int)*10); // Array for new keys
    float *updatedPointers = (float*)malloc(sizeof(int)*10); // Array for new pointers
    int currentPosition = 0;
    while(keyData.key > pgData->keys[currentPosition] && currentPosition < pgData->numberEntries){
        updatedKeys[currentPosition] = pgData->keys[currentPosition];
        updatedPointers[currentPosition] = pgData->pointers[currentPosition];
        currentPosition++;
    }

    if(keyData.key == pgData->keys[currentPosition] && currentPosition < pgData->numberEntries){
        return RC_IM_KEY_ALREADY_EXISTS;
    }
    else{
        updatedKeys[currentPosition] = keyData.key;
        updatedPointers[currentPosition] = keyData.left;
        updatedPointers[currentPosition+1] = keyData.right;
        currentPosition++;
        updatedKeys[currentPosition] = pgData->keys[currentPosition-1];
        currentPosition++;
        
    }

    // for (int j = currentPosition; j < page->numberEntries + 2; j++) {
    // updatedKeys[j] = page->keys[j - 1];
    // updatedPointers[j] = page->pointers[j - 1];
    // }
    while(currentPosition<pgData->numberEntries+2){
        updatedKeys[currentPosition]=pgData->keys[currentPosition-1];
        updatedPointers[currentPosition]=pgData->pointers[currentPosition-1];
        currentPosition++;
    }

    updatedPointers[currentPosition] = -1;
    free(pgData->keys);
    free(pgData->pointers);
    pgData->numberEntries++;
    pgData->pointers = updatedPointers;
    pgData->keys = updatedKeys;
   
    return RC_OK;
}

RC keyAndPointerDeletingInLeaf(pgData* pageData, int key){
    int keys[5] = {0},count = 0;
    float children[5] = {0};
    bool found = false;
    for(int i = 0;i < pageData->numberEntries;i++){
        if(key == pageData->keys[i] && i < pageData->numberEntries){
            //i++;
            found = true;
            continue;
        }
        keys[count] = pageData->keys[i];
        children[count] = pageData->pointers[i];
        //i++;
        count++;
    }
    if(!found){
        return RC_IM_KEY_NOT_FOUND;
    }
    //free(page->keys);
    //free(page->children);
    pageData->numberEntries -= 1;
    children[count] = -1;
    size_t index = 0;
    while(index < count)
    {
        pageData->keys[index] = keys[index];
        pageData->pointers[index] = children[index];
        index++;
    }
    pageData->pointers[count] = children[count];
    return RC_OK;
}

RC getLeafPg(pgData page,BM_BufferPool* bufferManager,BM_PageHandle* pageHandler,int* lPages){   
    
    if(!page.leaf){
        size_t childIndex = 0;
        while(childIndex<page.numberEntries+1)
        {
            pgData child;
            readPgData(bufferManager,pageHandler,&child,(int)page.pointers[childIndex]);
            if(getLeafPg(child,bufferManager,pageHandler,lPages) == RC_OK){
                childIndex++;
                continue;
            }
            childIndex++;
        }
    }
    else{
        lPages[counter] = page.pgNumber;
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
    SM_FileHandle fileHandler= ((treeData*)tree->mgmtData)->fileHandler;
    BM_PageHandle *pageHander = ((treeData*)tree->mgmtData)->pageHandler;
    BM_BufferPool *buffermanager= ((treeData*)tree->mgmtData)->bufferManager;

    pgData rootPgData;// root page data

    // getting the root page number
    int rootPgNumber=((treeData*)tree->mgmtData)->fMD.rootPgNumber;
    readPgData(buffermanager,pageHander,&rootPgData,rootPgNumber);

    pgData leafPageData= locatePageToInsertData(buffermanager,pageHander,rootPgData,key->v.intV);

    size_t index=0;

    while(index<leafPageData.numberEntries){
        if(leafPageData.keys[index] == key->v.intV){ // checking for the key
            
            float c=leafPageData.pointers[index];
            int cValue =round(c*10);
            int pgNumber= cValue /10;
            int slot=cValue%10;

            // updating slot and page number if key is found
            result->slot=slot;
            result->page=pgNumber;

            return RC_OK;
        }
        index++;
    }

    return RC_IM_KEY_NOT_FOUND;
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid){
    
    //printf("\nstart insert key\n");

    // getting the page handler, buffer manager and file handler
    BM_PageHandle *pageHandler = ((treeData*)tree->mgmtData)->pageHandler;
    BM_BufferPool *bufferManager = ((treeData*)tree->mgmtData)->bufferManager;
    SM_FileHandle fileHandler = ((treeData*)tree->mgmtData)->fileHandler;

    int maxEntry = ((treeData*)tree->mgmtData)->fMD.maxEntriesPerPage;
    int curNumOfNode = ((treeData*)tree->mgmtData)->fMD.numberOfNodes;
    int rootPgNum = ((treeData*)tree->mgmtData)->fMD.rootPgNumber;
    
    pgData rootPage; // getting the root page
    readPgData(bufferManager,pageHandler,&rootPage,rootPgNum);
    //printf("read pg data");

    pgData insertionPage; // getting the page where data can be inserted
    insertionPage = locatePageToInsertData(bufferManager,pageHandler,rootPage,key->v.intV);

    //printf("\nlocated page!------------\n");
    
    if(newkeyAndPtrToLeaf(&insertionPage,key->v.intV,rid) == RC_IM_KEY_ALREADY_EXISTS){
        return RC_IM_KEY_ALREADY_EXISTS;
    }
    
    //printf("\nnew key and ptr to leaf!------------\n");

    if(insertionPage.numberEntries > maxEntry){ // check if there is no space in the leaf node
        
        // creating new nodes
        int counter = 0;
        float *newNodechildren = (float *)malloc(10*sizeof(float));
        int *newNodeKeys = (int *)malloc(10*sizeof(int));
        
        for (size_t i = (int)ceil((insertionPage.numberEntries)/2)+1; i < insertionPage.numberEntries; i++)
        {
            newNodechildren[counter] = insertionPage.pointers[i];
            newNodeKeys[counter] = insertionPage.keys[i];
            counter++;
        }
        newNodechildren[counter] = -1;

        
        counter = 0;
        int *oldNodeKeys = (int *)malloc(10*sizeof(int));
        float *oldNodeChildren = (float *)malloc(10*sizeof(float));

        for (size_t i = 0; i <= (int)ceil((insertionPage.numberEntries)/2); i++)
        {
            oldNodeKeys[counter] = insertionPage.keys[i];
            oldNodeChildren[counter] = insertionPage.pointers[i];
            counter++;
        }
        oldNodeChildren[counter] = -1;

        ensureCapacity (curNumOfNode + 2, &fileHandler);

        curNumOfNode += 1;

        ((treeData*)tree->mgmtData)->fMD.numberOfNodes++;

        // setting data for right child
        pgData rightChild;
        rightChild.pgNumber = curNumOfNode;
        rightChild.leaf = 1;
        rightChild.pointers = newNodechildren;
        rightChild.numberEntries = (int)floor((maxEntry+1)/2);
        rightChild.keys = newNodeKeys;

        if(insertionPage.parentnode == -1)rightChild.parentnode = 3;
        else rightChild.parentnode = insertionPage.parentnode;

        //printf("start update right child");
        
        // updating data of right child
        char *dataHolder;
        allocateSpace(&dataHolder);
        preparePageDataToWrite(&rightChild,dataHolder);
        pageWrite(bufferManager,pageHandler,dataHolder,rightChild.pgNumber);
        deallocateSpace(&dataHolder);
        //printf("updated right child");


        // left child data
        pgData leftChild;
        leftChild.leaf = 1;
        leftChild.pgNumber = insertionPage.pgNumber;
        leftChild.numberEntries = (int)ceil((maxEntry+1)/2)+1;
        leftChild.keys = oldNodeKeys;
        leftChild.pointers = oldNodeChildren;
    
        if(insertionPage.parentnode == -1) leftChild.parentnode = 3;
        else leftChild.parentnode = insertionPage.parentnode;
        
        //printf("start update left child");
        
        // updating left child data
        allocateSpace(&dataHolder);
        preparePageDataToWrite(&leftChild,dataHolder);
        pageWrite(bufferManager,pageHandler,dataHolder,leftChild.pgNumber);
        deallocateSpace(&dataHolder);
        //printf("updated left child");

        int pgNumber = insertionPage.parentnode;
        float left = leftChild.pgNumber, right = rightChild.pgNumber;

        data keyData;
        keyData.left = left;
        keyData.key = rightChild.keys[0];
        keyData.right = right;

        //printf("progate start");
        propagateUp(tree,pgNumber,keyData); // propagate up
        //printf("progate end");
    }
    else{
        char *dataHolder = malloc(500);
        //printf("at root--------");
        allocateSpace(&dataHolder);
        preparePageDataToWrite(&insertionPage,dataHolder);
        pageWrite(bufferManager,pageHandler,dataHolder,insertionPage.pgNumber);
        deallocateSpace(&dataHolder);
        //printf("done root");
    }

    ((treeData*)tree->mgmtData)->fMD.numberOfEntries++; // change the number of entries

    forceFlushPool(bufferManager); // flush the buffer
    //printf("done key insert");
    return RC_OK;
    
}

// delete key
RC deleteKey (BTreeHandle *tree, Value *key){
    
    // getting buffer manager and page handler
    BM_BufferPool *bufferManager = ((treeData*)tree->mgmtData)->bufferManager;
    BM_PageHandle *pageHandler = ((treeData*)tree->mgmtData)->pageHandler;
    
    pgData rootPg; // getting the root page
    int rootPgIndex = ((treeData*)tree->mgmtData)->fMD.rootPgNumber;
    readPgData(bufferManager,pageHandler,&rootPg,rootPgIndex);

    pgData pageData  = locatePageToInsertData(bufferManager,pageHandler,rootPg,key->v.intV);

    if(keyAndPointerDeletingInLeaf(&pageData,key->v.intV) == RC_IM_KEY_NOT_FOUND) // deleting the key
        return RC_IM_KEY_NOT_FOUND; // if key not found

    char *updatedData;
    
    // updating the data
    allocateSpace(&updatedData);
    preparePageDataToWrite(&pageData,updatedData);
    pageWrite(bufferManager,pageHandler,updatedData,pageData.pgNumber);
    deallocateSpace(&updatedData);

    return RC_OK;
}

// open tree scan
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle){
    
    BM_PageHandle *pageHandler = ((treeData*)tree->mgmtData)->pageHandler;

    BM_BufferPool *bufferManager = ((treeData*)tree->mgmtData)->bufferManager;
    
    // allocating space for scan handler and scan manager
    scanMtdata = (scanData*)malloc(sizeof(scanData));
    scanHandle = (BT_ScanHandle*)malloc(sizeof(BT_ScanHandle));
    
    int rootPageNum = ((treeData*)tree->mgmtData)->fMD.rootPgNumber;

    pgData rootPg;
    readPgData(bufferManager,pageHandler,&rootPg,rootPageNum);
    
   // Allocate memory for storing leaf page numbers
    int *leafPages = (int *)malloc(100*sizeof(int));
    counter = 0;
    getLeafPg(rootPg,bufferManager,pageHandler,leafPages);
    //printf("\nInside tree scan!\n");
    scanMtdata->leafPage = leafPages;
    scanMtdata->curPage = leafPages[0];    
    pgData leafPage;
    readPgData(bufferManager,pageHandler,&leafPage,scanMtdata->curPage);
    scanMtdata->curPageData = leafPage;
    scanMtdata->nextPagePosInLeafPages = 1;
    scanMtdata->curPosInPage = 0;
    scanMtdata->noOfLeafPage = counter;
    scanMtdata->isCurPageLoaded = 1;

    scanHandle->mgmtData = scanMtdata;
    scanHandle->tree = tree;
    *handle = scanHandle;

    return RC_OK;
}

//next entry
RC nextEntry (BT_ScanHandle *handle, RID *result){
    
    BM_BufferPool *bufferManager = ((treeData*)handle->tree->mgmtData)->bufferManager;
    BM_PageHandle *pageHandler = ((treeData*)handle->tree->mgmtData)->pageHandler; 
    scanData* scanData = handle->mgmtData;

    if(scanData->curPosInPage >= scanData->curPageData.numberEntries){ // Check if the current position is beyond the number of entries on the current page
        // Check if there are no leaf pages to scan
        if(scanData->nextPagePosInLeafPages == -1 ){
            return RC_IM_NO_MORE_ENTRIES;
        }
        // Move to the next leaf page
        scanData->curPage = scanData->leafPage[scanData->nextPagePosInLeafPages];
        scanData->isCurPageLoaded = 0;
        scanData->nextPagePosInLeafPages += 1;

        if(scanData->nextPagePosInLeafPages >= scanData->noOfLeafPage){
            scanData->nextPagePosInLeafPages = -1;
        }
    }

    if(!scanData->isCurPageLoaded){
        pgData leafPg;
        readPgData(bufferManager,pageHandler,&leafPg,scanData->curPage);
        scanData->curPageData = leafPg;
        scanData->curPosInPage = 0;
        scanData->isCurPageLoaded = 1;   
    }

    // updating slot and page
    float c = scanData->curPageData.pointers[scanData->curPosInPage];
    int cValue = round(c*10);
    result->slot =  cValue % 10;
    result->page =  cValue / 10; 
    scanData->curPosInPage += 1;
    
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