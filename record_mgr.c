#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

// Record manager structure
typedef struct RecordManager
{
    BM_PageHandle pgManager;   // Page handler for buffer pool
    BM_BufferPool bufferPool;  // Buffer pool configuration
    RID recordID;              // Record ID
    Expr *condition;           // Condition expression
    int tupleCount;            // Number of tuples
    int firstPage;             // First page
    int scannedCount;          // Scanned count
} RecordManager;

const int max_page_num = 100;     // Maximum number of pages
const int attributeNameLength = 15;  // Maximum attribute name length

RecordManager *rm;  // Pointer to the record manager

// Function to find a free slot in a page
int findFreeSlot(char *data, int recordSize)
{
    int index = 0;
    // To get number of slots in page
    int slotsInPage = PAGE_SIZE / recordSize;

    while (index < slotsInPage)
    {
        if (data[index * recordSize] != '+')
            return index;
        index++;
    }
    return -1;
}

/*======================================================== Record Manager functions =================================================*/
// Initialize the record manager
extern RC initRecordManager(void *mgmtData)
{
    initStorageManager();
    return RC_OK;
}

// Shutdown the record manager
extern RC shutdownRecordManager()
{
    // Cleanup and deallocate resources
    rm = NULL;
    free(rm);
    return RC_OK;
}

/*======================================================== Table functions and operations =================================================*/
// Create a table with a given name and schema
extern RC createTable(char *name, Schema *schema)
{
    // Initialize the record manager
    rm = (RecordManager *)malloc(sizeof(RecordManager));

    // Initialize the buffer pool
    initBufferPool(&rm->bufferPool, name, max_page_num, RS_LRU, NULL);

    char pageContent[PAGE_SIZE];
    char *pgManager = pageContent;

    int operationResult, attributeIndex = 0;

    // Set initial values in the data page
    *(int *)pgManager = 0;  // Initializing the tuple count on the data page to 0
    pgManager = pgManager + sizeof(int); // Advancing the page manager to store next value

    *(int *)pgManager = 1;  // Sets the page number to 1
    pgManager = pgManager + sizeof(int);

    *(int *)pgManager = schema->numAttr;  // Sets number of attributes in schema to corresponding value in schema structure
    pgManager = pgManager + sizeof(int);

    *(int *)pgManager = schema->keySize;  // Sets key size
    pgManager = pgManager + sizeof(int);

    // Copy attribute names, data types, and type lengths
    while (attributeIndex < schema->numAttr)
    {
        strncpy(pgManager, schema->attrNames[attributeIndex], attributeNameLength);
        pgManager += attributeNameLength; // Move the pointer forward by attributeNameLength

        *(int *)pgManager = (int)schema->dataTypes[attributeIndex]; // Store the data type of the attribute as int
        pgManager += sizeof(int);

        *(int *)pgManager = (int)schema->typeLength[attributeIndex]; // Store the length of the attribute as int
        pgManager += sizeof(int);

        attributeIndex++;
    }

    SM_FileHandle fileHandle;

    // Create a new page file with the specified name
    if ((operationResult = createPageFile(name)) != RC_OK)
        return operationResult;

    // Open the newly created page file
    if ((operationResult = openPageFile(name, &fileHandle)) != RC_OK)
        return operationResult;

    // Write the initialized data to the first page of the file
    if ((operationResult = writeBlock(0, &fileHandle, pageContent)) != RC_OK)
        return operationResult;

    // Close the page file
    if ((operationResult = closePageFile(&fileHandle)) != RC_OK)
        return operationResult;

    return RC_OK;
}

// Open a table with the given name
extern RC openTable(RM_TableData *rel, char *name)
{
    SM_PageHandle pagePointer;

    int numAttributes, index = 0;

    rel->mgmtData = rm;
    rel->name = name;

    // Pin the first page to retrieve metadata
    pinPage(&rm->bufferPool, &rm->pgManager, 0);

    pagePointer = (char *)rm->pgManager.data;

    rm->tupleCount = *(int *)pagePointer;
    pagePointer = pagePointer + sizeof(int);

    rm->firstPage = *(int *)pagePointer;
    pagePointer = pagePointer + sizeof(int);

    numAttributes = *(int *)pagePointer;
    pagePointer = pagePointer + sizeof(int);

    Schema *schema;

    schema = (Schema *)malloc(sizeof(Schema));
    
    // Get data from record manager and assign those to schema
    schema->numAttr = numAttributes;
    schema->attrNames = (char **)malloc(sizeof(char *) * numAttributes);
    schema->dataTypes = (DataType *)malloc(sizeof(DataType) * numAttributes);
    schema->typeLength = (int *)malloc(sizeof(int) * numAttributes);

    while (index < numAttributes)
    {
        schema->attrNames[index] = (char *)malloc(attributeNameLength);
        index++;
    }

    index = 0;

    while (index < schema->numAttr)
    {
        strncpy(schema->attrNames[index], pagePointer, attributeNameLength);
        pagePointer += attributeNameLength;

        schema->dataTypes[index] = *(int *)pagePointer;
        pagePointer += sizeof(int);

        schema->typeLength[index] = *(int *)pagePointer;
        pagePointer += sizeof(int);

        index++;
    }

    rel->schema = schema;

    // Unpin the first page and force the changes to be saved
    unpinPage(&rm->bufferPool, &rm->pgManager);
    forcePage(&rm->bufferPool, &rm->pgManager);

    return RC_OK;
}

// Close a table
extern RC closeTable(RM_TableData *rel)
{
    RecordManager *rm = rel->mgmtData;

    // Shutdown the buffer pool
    shutdownBufferPool(&rm->bufferPool);

    return RC_OK;
}

// Delete a table with the given name
extern RC deleteTable(char *name)
{
    // Delete the page file
    destroyPageFile(name);
    return RC_OK;
}

// Get the number of tuples in a table
extern int getNumTuples(RM_TableData *rel)
{
    RecordManager *rm = rel->mgmtData;
    return rm->tupleCount;
}

// Insert a record into the table
extern RC insertRecord(RM_TableData *rel, Record *record)
{
    // Extract the information
    RecordManager *rm = rel->mgmtData;
    RID *rid = &record->id;
    char *pageData, *recordPosition;

    int sizeOfRecord = getRecordSize(rel->schema);

    // Assign the page to the recordID from the record manager
    rid->page = rm->firstPage;

    // Pin the page where the record will be inserted
    pinPage(&rm->bufferPool, &rm->pgManager, rid->page);

    pageData = rm->pgManager.data;

    // Find a free slot for the record
    rid->slot = findFreeSlot(pageData, sizeOfRecord);

    while (rid->slot == -1)
    {
        // If there are no free slots, unpin the page, move to the next page, and try again
        unpinPage(&rm->bufferPool, &rm->pgManager);
        rid->page++;

        pinPage(&rm->bufferPool, &rm->pgManager, rid->page);
        pageData = rm->pgManager.data;

        rid->slot = findFreeSlot(pageData, sizeOfRecord);
    }

    recordPosition = pageData;

    // Mark the slot where record will be inserted
    markDirty(&rm->bufferPool, &rm->pgManager);
    recordPosition += rid->slot * sizeOfRecord;
    *recordPosition = '+';

    // Copy the record data to the slot
    memcpy(++recordPosition, record->data + 1, sizeOfRecord - 1);

    // Unpin the page
    unpinPage(&rm->bufferPool, &rm->pgManager);

    // Update the tuple count to reflect the new records
    rm->tupleCount++;

    // Pin the first page to update metadata
    pinPage(&rm->bufferPool, &rm->pgManager, 0);

    return RC_OK;
}

// Delete a record with the given RID
extern RC deleteRecord(RM_TableData *rel, RID id)
{
    RecordManager *rm = rel->mgmtData;

    // Pin the page containing the record
    pinPage(&rm->bufferPool, &rm->pgManager, id.page);

    // Set the page as the first page to delete
    rm->firstPage = id.page;

    // Retrieve the pointer data
    char *pageData = rm->pgManager.data;
    int recordSize = getRecordSize(rel->schema);

    // Mark the slot as deleted by writing '-'
    pageData += (id.slot * recordSize);
    *pageData = '-';

    // Mark the page as dirty to ensure changes are made
    markDirty(&rm->bufferPool, &rm->pgManager);

    // Unpin the page
    unpinPage(&rm->bufferPool, &rm->pgManager);

    return RC_OK;
}

// Update a record with the given RID
extern RC updateRecord(RM_TableData *rel, Record *record)
{
    RecordManager *recordManager = rel->mgmtData;

    // Pin the page containing the record
    pinPage(&recordManager->bufferPool, &recordManager->pgManager, record->id.page);

    // Accessing record data
    char *data;
    int recordSize = getRecordSize(rel->schema);
    RID id = record->id;

    data = recordManager->pgManager.data;
    data = data + (id.slot * recordSize);

    // Mark the slot as used
    *data = '+';

    // Copy the updated record data to the slot
    memcpy(++data, record->data + 1, recordSize - 1);

    // Mark the page as dirty
    markDirty(&recordManager->bufferPool, &recordManager->pgManager);

    // Unpin the page
    unpinPage(&recordManager->bufferPool, &recordManager->pgManager);

    return RC_OK;
}

// Get a record with the given RID
extern RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    RecordManager *rm = rel->mgmtData;

    // Pin the page containing the record
    pinPage(&rm->bufferPool, &rm->pgManager, id.page);

    int sizeOfRecord = getRecordSize(rel->schema);
    char *pageData = rm->pgManager.data;
    pageData = pageData + (id.slot * sizeOfRecord);

    // Check if the slot is in use
    while (*pageData != '+')
    {
        // No tuple found with the given RID
        unpinPage(&rm->bufferPool, &rm->pgManager);
        return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
    }

    // Copy the record data to the output record
    record->id = id;
    char *data = record->data;
    memcpy(++data, pageData + 1, sizeOfRecord - 1);

    // Unpin the page
    unpinPage(&rm->bufferPool, &rm->pgManager);

    return RC_OK;
}


//**************************************************************************************************************************************************
//Scan

extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    if(cond == NULL){ 
        return RC_SCAN_CONDITION_NOT_FOUND;
    }
    openTable(rel,"ScanTable");
    RecordManager *scanManager = (RecordManager *)malloc(sizeof(RecordManager));
    RecordManager *tableManager;

    scan -> mgmtData = scanManager;
    
    scanManager -> recordID.page=1;
    scanManager -> recordID.slot=0;
    scanManager -> scannedCount=0;
    scanManager -> condition = cond;

    tableManager = rel -> mgmtData;
    tableManager -> tupleCount = attributeNameLength;
    scan -> rel=rel;

    return RC_OK;
}

extern RC next (RM_ScanHandle *scan, Record *record){

    RecordManager *scanManager = scan -> mgmtData;
    RecordManager *tableManager = scan -> rel -> mgmtData;
    Schema *schema = scan -> rel -> schema;

    if (scanManager -> condition == NULL){
        return RC_SCAN_CONDITION_NOT_FOUND; 
    }

    Value *final = (Value *) malloc(sizeof(Value));
   
    int size = getRecordSize(schema);
    int count = PAGE_SIZE/size;
    
    int scanCount = scanManager -> scannedCount;
    int tupCount = tableManager -> tupleCount;

    if (tupCount == 0){
        return RC_RM_NO_MORE_TUPLES;
    }

    while(scanCount<=tupCount){  
        if (scanCount <= 0){
            scanManager -> recordID.page = 1;
            scanManager -> recordID.slot = 0;
        }
        else{
            scanManager -> recordID.slot++;
            if(scanManager -> recordID.slot >= count){
                scanManager -> recordID.slot = 0;
                scanManager -> recordID.page = scanManager -> recordID.page + 1;
            }
        }

        pinPage(&tableManager -> bufferPool,&scanManager -> pgManager,scanManager -> recordID.page);
        char *data = scanManager->pgManager.data + (scanManager->recordID.slot * size);   
 
        record -> id.page = scanManager->recordID.page;
        record -> id.slot = scanManager->recordID.slot;

        char *dataRef = record->data;
        *dataRef = '-';
        memcpy(++dataRef, data + 1, size - 1);
        record -> data[0] = '-';

        scanManager -> scannedCount++;
        scanCount++;

        evalExpr(record,schema,scanManager -> condition,&final); 

        if(final -> v.boolV == TRUE){
            unpinPage(&tableManager->bufferPool, &scanManager->pgManager);
            return RC_OK;
        }
    }
    
    unpinPage(&tableManager -> bufferPool, &scanManager -> pgManager);
    scanManager->recordID.page = 1;
    scanManager->recordID.slot = 0;
    scanManager->scannedCount = 0;
    
    return RC_RM_NO_MORE_TUPLES;    
}

extern RC closeScan (RM_ScanHandle *scan){
    RecordManager *scanManager=scan -> mgmtData;
    RecordManager *recordManager=scan -> rel -> mgmtData;

    switch(scanManager -> scannedCount > 0){
        case 1:
            unpinPage(&recordManager -> bufferPool,&scanManager -> pgManager);
            scanManager -> scannedCount=0;
            scanManager -> recordID.page=1;
            scanManager -> recordID.slot=0;
    }
    scan -> mgmtData= NULL;
    
    free(scan -> mgmtData);  
    return RC_OK;
}

//**************************************************************************************************************************************************

//Schemas

extern int getRecordSize (Schema *schema){
    int totalSize = 0, iter=0; 
    
    while(iter<schema -> numAttr){
        if(schema -> dataTypes[iter]==DT_STRING ){
            totalSize += schema->typeLength[iter];
        }
        else if(schema -> dataTypes[iter]==DT_INT ){
            totalSize += sizeof(int);
        }
        else if(schema -> dataTypes[iter]==DT_FLOAT ){
            totalSize += sizeof(float);
        }
        else if(schema -> dataTypes[iter]==DT_BOOL ){
            totalSize += sizeof(bool);
        }
        iter++;
    }
    return totalSize+1;
}

extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
      
    Schema *new_schema=(Schema *)malloc(sizeof(Schema));
    
    new_schema -> numAttr = numAttr;
    new_schema -> attrNames = attrNames;
    new_schema -> dataTypes = dataTypes;
    new_schema -> typeLength = typeLength;
    new_schema -> keySize = keySize;   
    new_schema -> keyAttrs = keys;
    return new_schema; 
}

extern RC freeSchema (Schema *schema){
    free(schema);   //memory free up
    return RC_OK;
}

//**************************************************************************************************************************************************

//Records and attribute values

extern RC createRecord (Record **record, Schema *schema){
    
    Record *record_new=(Record*)malloc(sizeof(Record));
    
    int record_size = getRecordSize(schema);
    
    record_new -> data= (char*)malloc(record_size);
    record_new -> id.page = record_new->id.slot = -1;
   
    char *Ref = record_new -> data;
    
    *Ref = '-';
    *(++Ref) = '\0';
    *record = record_new;

    return RC_OK;
}

extern RC freeRecord (Record *record){
    free(record);   //memory free up
    return RC_OK;
}

extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    int offset = 0;
    build(schema,attrNum,&offset);

    Value *attr = (Value*)malloc(sizeof(Value));

    char *dataRef = record -> data + offset;

    schema->dataTypes[attrNum] = (attrNum == 1) ? 1 : schema->dataTypes[attrNum];
    
    if(schema -> dataTypes[attrNum]==DT_INT){
        int val = 0;
        memcpy(&val, dataRef, sizeof(int));
        attr->v.intV = val;
        attr->dt = DT_INT;
    } 
    
    else if(schema -> dataTypes[attrNum]==DT_FLOAT){
        float val;
        memcpy(&val, dataRef, sizeof(float));
        attr->v.floatV = val;
        attr->dt = DT_FLOAT;
    } 
    
    else if(schema -> dataTypes[attrNum]==DT_BOOL){
        bool val;
        memcpy(&val, dataRef, sizeof(bool));
        attr->v.boolV = val;
        attr->dt = DT_BOOL;
    } 
    
    else if(schema -> dataTypes[attrNum]==DT_STRING){
        int len = schema->typeLength[attrNum];
        attr->v.stringV = (char *)malloc(len + 1);
        strncpy(attr->v.stringV, dataRef, len);
        attr->v.stringV[len] = '\0';
        attr->dt = DT_STRING;
    } 
    
    else {
        printf("Unknown Serializer for given datastructure.\n");
    }

    *value = attr;
    return RC_OK;
}

extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    int offset = 0;
    build(schema,attrNum,&offset);

    char *attrData = record -> data + offset;
        
    if (schema -> dataTypes[attrNum] == DT_FLOAT){
        *(float *)attrData = value -> v.floatV;
        attrData = attrData + sizeof(float);
    } 
    
    else if (schema -> dataTypes[attrNum] == DT_STRING){
        int len = schema -> typeLength[attrNum];
        strncpy(attrData, value -> v.stringV, len);
        attrData = attrData + schema -> typeLength[attrNum];
    } 
    
    else if (schema->dataTypes[attrNum] == DT_BOOL){
        *(bool *)attrData = value -> v.boolV;
        attrData = attrData + sizeof(bool);
    } 
    
    else if (schema -> dataTypes[attrNum] == DT_INT){
        *(int *)attrData = value -> v.intV;
        attrData = attrData + sizeof(int);
    } 
    
    else{
        printf("Unknown Serializer for given datastructure.\n");
    }       
    return RC_OK;
}

RC build (Schema *schema, int attrNum, int *result)
{
    int iter = 0;
    *result = 1;

    while (iter < attrNum) {
        if (schema -> dataTypes[iter] == DT_STRING){
            *result+=schema->typeLength[iter];
        } 
        
        else if (schema -> dataTypes[iter] == DT_INT){
            *result+=sizeof(int);
        } 
        
        else if (schema -> dataTypes[iter] == DT_FLOAT){
            *result+=sizeof(float);
        } 
        
        else if (schema -> dataTypes[iter] == DT_BOOL){
            *result+=sizeof(bool);
        }

        iter++;
    }
    return RC_OK;
}

//**************************************************************************************************************************************************