#include "storage_mgr.h"
#include "dberror.h"
#include<sys/stat.h>
#include <stdio.h>
#include <stdlib.h>

FILE *globalFile;

// dummy function, as it has no use we have left it empty
extern void initStorageManager (){ } // empty as we have no use for this

// creating a single page
extern RC createPageFile(char *fileName){
    globalFile=fopen(fileName,"w+"); // open file in read and write mode
    //printf("File is opened!...........\n");

    if(globalFile==NULL){ // checking whether the file exist or not
        //printf("File create failed!........\n");
        return RC_WRITE_FAILED;
    }

    // creating new a page in a fixed block size
    SM_PageHandle newPage=(SM_PageHandle)calloc(PAGE_SIZE,sizeof(char));

    if(newPage==NULL){
        fclose(globalFile); // closing the file as page create failed
        //printf("Page create failed!.......");
        return RC_WRITE_FAILED;
    }

    //printf("Page is created!......");

    //adding the page into the file
    int writtenBlockSize=fwrite(newPage,sizeof(char),PAGE_SIZE,globalFile);

    if(writtenBlockSize<PAGE_SIZE){ //out of space in file to write all data
        //printf("Out of space in file!......");
        free(newPage); // remove the allocated memory
        fclose(globalFile); // close the file
        return RC_WRITE_FAILED;
    }

    fclose(globalFile); //close the file
    
    free(newPage); // free the allocated space
   
    return RC_OK;
}

// opening page file
extern RC openPageFile(char *fileName, SM_FileHandle *fHandle){ 
    globalFile=fopen(fileName,"r"); // opening the a binary file in read and write mode
    
    if(globalFile==NULL){ // check whether the file exist or not
        //printf("File not found!");
        return RC_FILE_NOT_FOUND;
    }

    struct stat info;
    int stat=fstat(fileno(globalFile),&info); // getting file info

    if(stat<0) return RC_ERROR; // checking whether info is found

    //setting other metadata
    fHandle->totalNumPages=info.st_size/PAGE_SIZE; // setting total page size
    fHandle->fileName=fileName; // setting file name
    fHandle->curPagePos=0; // setting current position

    fclose(globalFile);

    return RC_OK;
}

//closing page file
extern RC closePageFile(SM_FileHandle *fHandle){
  
    if(globalFile!=NULL) // check if file is present
        globalFile=NULL; // if exist, setting it to NULL
    return RC_OK;
}

//delete page file
extern RC destroyPageFile(char *fileName){
    
    globalFile=fopen(fileName,"r"); // opening the file in read mode, to check its existence 
    
    if(globalFile==NULL){
        //printf("File Destroy: file not found!");
        return RC_FILE_NOT_FOUND;
    }

    if(fclose(globalFile)==0){ // checking whether the file is closed
        //printf("file closed\n");
        remove(fileName); // deleting the file
    }
    else{
         //printf("file not closed");
         return RC_FILE_NOT_FOUND;
    }
    
    return RC_OK; 
}

// reading a block
extern RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

    // Checking whether the file handle exists
    if( pageNum < 0 || pageNum > fHandle->totalNumPages) {
        //printf("No proper file exists!");
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Get the file
   globalFile = fopen(fHandle->fileName,"r");

    // Get the position of the file to begin the read
    long pos = (long) pageNum * PAGE_SIZE;

    // Check if the read position is in right direction
    if(fseek(globalFile,pos,SEEK_SET) != 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // add the read page data into mempage
    size_t bRead = fread(memPage, sizeof(char), PAGE_SIZE, globalFile);

    //printf("An error occured when attempting read");
    if(bRead<PAGE_SIZE) return RC_READ_NON_EXISTING_PAGE; // checking if the file is read

    // Update the read page position in the file handle
    fHandle->curPagePos = ftell(globalFile);

    fclose(globalFile);

    return RC_OK;
}

// get a block position
extern RC getBlockPos(SM_FileHandle *fHandle) {
    // Gets the block position in the fHandle
    return fHandle->curPagePos;
}

//read first block
extern RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Gets the block with page number 0 i.e First
    return readBlock(0, fHandle, memPage);
}

// read pervious block
extern RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Gets the one before the current block in the fHandle i.e Previous
    return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

// read current block
extern RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Gets the current block in the fHandle
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

// read next block
extern RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Gets the one after the current block in the fHandle i.e Next
    return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
}

// read last block
extern RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Gets the last block in the fHandle
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

// write a block
extern RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) 
{
    /*Verifying if the given pageNum is valid*/
    if(pageNum < 0 || pageNum > fHandle -> totalNumPages)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }

    globalFile=fopen(fHandle->fileName,"r+"); // open the file

    /*Calculating the sum to required page*/
    int sum = pageNum * PAGE_SIZE;

    if(pageNum!=0){ // if page number is not zero
        fHandle->curPagePos=sum; // setting the page position
        fclose(globalFile); // closing the file
        writeCurrentBlock(fHandle,memPage); // writing the block
    }
    else{ // if it's the first page
        if(fseek(globalFile, sum, SEEK_SET) != 0) // seek the pointer 
        {
            fclose(globalFile);
            return RC_WRITE_FAILED;
        }

        /*Writing data from memPage to file*/   
        for(int i=0;i<PAGE_SIZE;i++){
            if(feof(globalFile)) appendEmptyBlock(fHandle);
            fputc(memPage[i],globalFile);
        }

        fHandle->curPagePos=ftell(globalFile); // update the position in page handler
        fclose(globalFile); // close the file
    }

    return RC_OK;
}

// write in the current block
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    globalFile=fopen(fHandle->fileName,"r+"); // opening the file in read and write mode

    if(globalFile!=NULL){ // if file exist

        appendEmptyBlock(fHandle); // appending empty block

        fseek(globalFile,fHandle->curPagePos,SEEK_SET); // seeking the pointer in the file

        fwrite(memPage,sizeof(char),strlen(memPage),globalFile); // writting the data

        fHandle->curPagePos=ftell(globalFile); // updating the postion

        fclose(globalFile); // closing the file

        return RC_OK;
    }
    return RC_FILE_NOT_FOUND; // if file not found
}

// appending empty block
extern RC appendEmptyBlock (SM_FileHandle *fHandle)
{

    SM_PageHandle newblock = (SM_PageHandle)calloc(PAGE_SIZE,sizeof(char)); // allocation memory for new block

    if(fseek(globalFile,0,SEEK_END) != 0){ // moving the pointer
        free(newblock);
        return RC_WRITE_FAILED; // if there is issue in writting the file
    }
    else{
        fwrite(newblock,sizeof(char),PAGE_SIZE,globalFile); // empty block is created
    }

    free(newblock);
    fHandle->totalNumPages++; // updating the total page number
    return RC_OK;

}

// checking the capacity
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{

    globalFile=fopen(fHandle->fileName,"a"); // opening the file and pointer position will at EOF if there is any data

    while(numberOfPages>fHandle->totalNumPages) // if the given page number is less than total page number, adding blocks till they become unequal
        appendEmptyBlock(fHandle);
    
    fclose(globalFile); // closing the file

    return RC_OK;
}