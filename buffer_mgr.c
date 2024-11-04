#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"

typedef struct PgFrame // Data of a frame
{
    PageNumber pgNumber; // page number
    SM_PageHandle pageData; // page Handler
    bool isDirty; // flag for dirty
    int pageCounter; // page in use, count of fixed pages in buffer
    int leastrecentlyUsedPage; // least recently used page number for LRU
    int leastFrequentlyUsedPage; // least frequently used page number fir LFU

} PgFrame;

int bufferSize=0; // global buffer size
int diskWritten=0; // number times the disk is written
int diskRead=0; // number of pages read from disk
int lastPageInClock=0; // last page used in clock
int lastPageInLFU=0; // last page used in LFU
int cache=0; // to track cache hits

SM_FileHandle fh; // global file handler

/*=================================================================buffer pool functions=======================================================================*/

//initialising the buffer pool
extern RC initBufferPool(BM_BufferPool *const bm,
                        const char * const pageFileName, const int numPages,
                        ReplacementStrategy strategy, void *stratData){
    
    // initialising the buffer
    bm->numPages=numPages;
    bm->pageFile=(char *) pageFileName;
    bm->strategy=strategy;

    PgFrame *pageFrames=malloc(sizeof(PgFrame)*numPages); // creating the memory frames
    bufferSize=numPages; // initalizing the buffer size

    int index=0;

    while(index < bufferSize ){ // for each frame setting the default value
        pageFrames[index].pageCounter=0;
        pageFrames[index].isDirty=FALSE;
        pageFrames[index].leastrecentlyUsedPage=0;
        pageFrames[index].leastFrequentlyUsedPage=0;
        pageFrames[index].pageData=NULL;
        pageFrames[index].pgNumber=-1;
        index++;
    }

    bm->mgmtData= pageFrames; // setting the frames to management data

    // counters for replacement algorithms
    diskWritten = 0;
    lastPageInClock = 0;
    lastPageInLFU = 0;
    
    return RC_OK;

}

// to flush out all the pages from the buffer pool
extern RC forceFlushPool(BM_BufferPool *const bm){
    
    PgFrame *pageFrames=(PgFrame*) bm->mgmtData; // gettting pageframes from buffer pool

    int index=0;
    
    while(index<bufferSize){
        if(pageFrames[index].isDirty==TRUE && pageFrames[index].pageCounter==0){ // checking whether the page is dirty and not in use
            // if page is dirty, it must be written in the disk
            //SM_FileHandle fh;
            openPageFile(bm->pageFile,&fh); // opening the page file
            writeBlock(pageFrames[index].pgNumber,&fh,pageFrames[index].pageData); // writing the content into the disk
            pageFrames[index].isDirty=FALSE; // setting the frame as not dirty
            diskWritten++; // incrementing disk written count
        }
        index++;
    }
    return RC_OK;
}

// to shutdown buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm){
    
    PgFrame *pageFrames=(PgFrame *) bm->mgmtData; // getting the page frames from the buffer pool
    //printf("start force flush");
    forceFlushPool(bm); // flushing the buffer before shutting it down.
    //printf("done force flush");
    int index=0;

    while(index < bufferSize){
        //printf("%d\n",pageFrames[index].pageCounter);
        if(pageFrames[index].pageCounter!=0){ // checking whether page is in use or not
            return RC_ERROR;
        }
        index++;
    }
    //printf("done shutdown");

    free(pageFrames); // freeing the memory

    bm->mgmtData = NULL; // removing the data from mgmtData

    return RC_OK;

}

/*====================================================================Page Replacement Strategy=================================================================*/

// First In First Out replacement algorithm 
void FIFO(BM_BufferPool *const bm, PgFrame * page){
    PgFrame *pageFrames=(PgFrame*)bm->mgmtData; // getting the page frames from buffer pool

    int index=0, startIndex;

    startIndex= diskRead % bufferSize; // finding the initial index

    while(index < bufferSize){
        if(pageFrames[startIndex].pageCounter==0){
            if(pageFrames[startIndex].isDirty==TRUE){ // if the page is dirty, writting it in the disk
                //SM_FileHandle fh;
                openPageFile(bm->pageFile,&fh);
                writeBlock(pageFrames[startIndex].pgNumber,&fh,pageFrames[startIndex].pageData);
                diskWritten++;
            }
            // changing frame with new frame in the buffer
            pageFrames[startIndex].pageData=page->pageData;
            pageFrames[startIndex].isDirty=page->isDirty;
            pageFrames[startIndex].pgNumber=page->pgNumber;
            pageFrames[startIndex].pageCounter=page->pageCounter;
            break;
        }
        else{
            startIndex++;
            if(startIndex % bufferSize==0) startIndex=0; // restarting the loop if we are at end of the buffer
        }
        //free(pageFrames);
        index++;
    }
}

// LFU (Least Frequently Used) page replacement srategy
extern void LFU(BM_BufferPool *const bm, PgFrame *poolFrame) {
   
    int index1=0, index2=0; // for loops
    int leastFreqIndex = lastPageInLFU, minFreqCount; // storing the value of LFU index
    PgFrame *f = (PgFrame*) bm -> mgmtData; // Retrieve the array of frames from the buffer pool management data.

    while(index1 < bufferSize) {
        // Check if the page in the current frame is not fixed
        if(f[leastFreqIndex].pageCounter == 0) {
            // Find the frame with least frequent usage (LFU)
            leastFreqIndex = (leastFreqIndex + index1) % bufferSize;
            minFreqCount = f[leastFreqIndex].leastFrequentlyUsedPage;
            break;
        }
        index1++;
    }
    // Pointer traversal across the buffer frame
    index1 = (leastFreqIndex + 1) % bufferSize;
    
    while(index2 < bufferSize) {
        if(f[index1].leastFrequentlyUsedPage < minFreqCount) {
            // Update the LFU index if a frame with lower LFU count is found
            leastFreqIndex = index1;
            minFreqCount = f[index1].leastFrequentlyUsedPage;
        }
        index1 = (index1 + 1) % bufferSize;
        index2++;
    }
    
    if(f[leastFreqIndex].isDirty == 1) {
        // If it's dirty, write the page to the disk before replacing it
        openPageFile(bm->pageFile, &fh);
        writeBlock(f[leastFreqIndex].pgNumber, &fh, f[leastFreqIndex].pageData);
        diskWritten++;
    }
    
    // Update the page information with the new page frame
    f[leastFreqIndex].isDirty = poolFrame -> isDirty;
    f[leastFreqIndex].pageCounter = poolFrame -> pageCounter;
    f[leastFreqIndex].pageData = poolFrame -> pageData;
    f[leastFreqIndex].pgNumber = poolFrame -> pgNumber;
    
    // Update the LFU pointer to the next frame
    lastPageInLFU = leastFreqIndex + 1;
}

// LRU (Least Recently Used) page replacement strategy
extern void LRU(BM_BufferPool *const bm, PgFrame *poolFrame) {
    // Retrieve the array of frames from the buffer pool management data.
    PgFrame *f = (PgFrame*) bm -> mgmtData;
    int lastHitIndex, minCacheCount;
    int index=0;

    // Get the first frame with the least recently used (LRU) count
    while(index < bufferSize) {
        // Check if the page in the current frame is not fixed
        if(f[index].pageCounter == 0) {
            lastHitIndex = index;
            minCacheCount = f[index].leastrecentlyUsedPage;
            break;
        }
        index++;
    }    

    index= lastHitIndex+1;

    // Go through the frames to find the frame with the lowest LRU count
    while(index < bufferSize) {
        if(f[index].leastrecentlyUsedPage < minCacheCount) 
        {
            lastHitIndex = index;
            minCacheCount = f[index].leastrecentlyUsedPage;
        }
        index++;
    }

    if(f[lastHitIndex].isDirty == TRUE) {
        // If it's dirty, write the page to the disk before replacing it
        SM_FileHandle fh;
        openPageFile(bm -> pageFile, &fh);
        writeBlock(f[lastHitIndex].pgNumber, &fh, f[lastHitIndex].pageData);
        diskWritten++;
    }
    
    // Update the page information with the new page frame
    f[lastHitIndex].pgNumber = poolFrame -> pgNumber;
    f[lastHitIndex].isDirty = poolFrame -> isDirty;
    f[lastHitIndex].pageCounter = poolFrame -> pageCounter;
    f[lastHitIndex].pageData = poolFrame -> pageData;
    f[lastHitIndex].leastrecentlyUsedPage = poolFrame -> leastrecentlyUsedPage;
}

// CLOCK page replacement strategy
extern void CLOCK(BM_BufferPool *const bm, PgFrame *poolFrame) {
    
    // Retrieve the array of frames from the buffer pool management data.
    PgFrame *f = (PgFrame*) bm -> mgmtData;

    // Infinite loop for CLOCK replacement.
    while(1) {
        // Ensure circular traversal of frames for CLOCK algorithm.
        // If clkIndex reaches the end of the array, wrap it around to 0.
        if(lastPageInClock % bufferSize == 0) lastPageInClock=0;    
   
        if(f[lastPageInClock].leastrecentlyUsedPage == 0) {
            if(f[lastPageInClock].isDirty == TRUE) {
                // If it's dirty write the page to the disk before replacing it.
                openPageFile(bm -> pageFile, &fh);
                writeBlock(f[lastPageInClock].pgNumber, &fh, f[lastPageInClock].pageData);
                diskWritten++;
            }
            
            // Update the page infomation with the new page frame.
            f[lastPageInClock].pageData = poolFrame -> pageData;
            f[lastPageInClock].isDirty = poolFrame -> isDirty;
            f[lastPageInClock].pageCounter = poolFrame -> pageCounter;
            f[lastPageInClock].pgNumber = poolFrame -> pgNumber;
            f[lastPageInClock].leastrecentlyUsedPage = poolFrame -> leastrecentlyUsedPage;
            
            lastPageInClock++;
            break;    
        }
        else 
            poolFrame[lastPageInClock++].leastrecentlyUsedPage = 0;     // Reset the LRU count for the current frame.
    }
}


/*====================================================================Page Management Functions====================================================================*/ 

// to make a page as dirty
extern RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    //the page handler has modified the contents of frame

    PgFrame* ptr =(PgFrame*) bm -> mgmtData;
    for(int i = 0; i < bufferSize; i++)
    {
        if(ptr[i].pgNumber == page -> pageNum) // check for the page
        {
            ptr[i].isDirty = TRUE; // if page is found marking it as dirty
            return RC_OK;
        }
    }
    //unable to find page in buffer pool!!
    return RC_ERROR;
}

// to unpin the page
extern RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    PgFrame* ptr = (PgFrame*)bm -> mgmtData;
    for(int i = 0; i < bufferSize; i++)
    {
        //look through the page table to find pageNum because page numbers and page frames may not be the same
        if(ptr[i].pgNumber == page -> pageNum)
        {
            //printf("Page in use value %d\n",ptr[i].pageCounter);
            ptr[i].pageCounter--;
            return RC_OK;
        }
    }
    //unable to find the page!!!
    //printf("page not found");
    return RC_ERROR;
}

//  forcing a page to write in the disk
extern RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
   
    //find the row in the pagetable
    PgFrame *ptr = (PgFrame*)bm -> mgmtData;
    for(int i = 0; i < bufferSize; i++)
    {
        if(ptr[i].pgNumber == page -> pageNum)
        {
            //SM_FileHandle fh;
            openPageFile(bm -> pageFile, &fh);

            //write data to fhandler
            writeBlock(ptr[i].pgNumber, &fh, ptr[i].pageData);
            
            //mark page as clean
            ptr[i].isDirty = FALSE;
            
            diskWritten++;
        }
    }
    //page number not found in buffer pool!!!
    return RC_OK;
}

// to pin a page in the buffer pool
extern RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    //printf("\npage pinned!\n");
    //update pin counter in page table
    PgFrame *ptr = (PgFrame*)bm -> mgmtData;
    if(ptr[0].pgNumber != -1){ // first page is available
       bool bufferOverflow = true;
        for(int i = 0; i < bufferSize; i++)
        {
            if(ptr[i].pgNumber!=-1){ // if page exist
               
                if(ptr[i].pgNumber == pageNum) // if page is found
                {
                    ptr[i].pageCounter++; // increasing the page counter
                    bufferOverflow = false; // setting the buffer as not full
                    cache++; // increasing cache hits
                
                    // updating flags of page replacement algorithms
                    if(bm->strategy==RS_LRU) ptr[i].leastrecentlyUsedPage= cache;
                    else if(bm->strategy==RS_CLOCK) ptr[i].leastrecentlyUsedPage=1;
                    else if(bm->strategy==RS_LFU) ptr[i].leastFrequentlyUsedPage++;

                    // Output data
                    page->pageNum= pageNum; // setting the page number
                    page->data = ptr[i].pageData; // setting the page handler data

                    lastPageInClock++; // move the clock pointer

                    break;
                }
            }
            else{
                SM_FileHandle fh;
                openPageFile(bm->pageFile, &fh);

                ptr[i].pageData = (SM_PageHandle) malloc(PAGE_SIZE); // allocation of page data

                readBlock(pageNum,&fh,ptr[i].pageData); // reading the page data
                
                ptr[i].pgNumber = pageNum; // updating the page number
                ptr[i].pageCounter =1; // setting the page counter
                ptr[i].leastFrequentlyUsedPage=0; // for LFU
                diskRead++;
                cache++;

                //updating based on the strategy
                if(bm->strategy==RS_CLOCK) ptr[i].leastrecentlyUsedPage=1;
                else if(bm->strategy==RS_LRU) ptr[i].leastrecentlyUsedPage=cache;

                
                bufferOverflow=false; 

                // output data
                page->pageNum=pageNum;
                page->data = ptr[i].pageData;
                
                break;

            }
        }

        if(bufferOverflow==true){ // if buffer is full
            
        PgFrame *pageFrame=(PgFrame*)malloc(sizeof(PgFrame)); // allocation page frame memory
        //SM_FileHandle fh;
        openPageFile(bm->pageFile,&fh); // open the page file
        pageFrame->pageData = (SM_PageHandle) malloc(PAGE_SIZE); // allocate memory for page data
        readBlock(pageNum,&fh,pageFrame->pageData); // reading the data into buffer
        pageFrame->leastFrequentlyUsedPage=0; // for LFU
        pageFrame->pgNumber=pageNum; // setting page number
        pageFrame->isDirty=FALSE; // marking page as not dirty
        pageFrame->pageCounter=1; // setting the page counter
        diskRead++; // increasing the disk read count
        cache++; // increasing cache hits

        // for page replacement 
        if(bm->strategy==RS_CLOCK) pageFrame->leastrecentlyUsedPage=1;
        else if(bm->strategy==RS_LRU) pageFrame->leastrecentlyUsedPage=cache;

        // output data
        page->pageNum=pageNum;
        page->data= pageFrame->pageData;

        // selecting the strategy
        switch(bm->strategy){
            case RS_FIFO:
                FIFO(bm,pageFrame);
                break;
            case RS_CLOCK:
                CLOCK(bm,pageFrame);
                break;
            case RS_LRU:
                LRU(bm,pageFrame);
                break;
            case RS_LFU:
                LFU(bm,pageFrame);
                break;
            default:
                printf("Strategy not found");
                break;
            }
            free(pageFrame);
        }
        //printf("page pinned done!");
        return RC_OK;
    }
    else{ // if first page is empty
     //SM_FileHandle fh;
        SM_FileHandle fh;
        openPageFile(bm->pageFile,&fh);

        ptr[0].pageData = (SM_PageHandle) malloc(PAGE_SIZE); // providing memory for page data
        ensureCapacity(pageNum,&fh); // ensuring the capacity
        readBlock(pageNum,&fh,ptr[0].pageData); // reading the page data into buffer
        
        // setting the meta data
        ptr[0].pageCounter++;
        ptr[0].pgNumber=pageNum;
        diskRead = cache = 0;
        ptr[0].leastrecentlyUsedPage = cache;
        
        page->pageNum=pageNum; // setting the output page number
        page->data=ptr[0].pageData; // setting the output data
        //printf("page pinned done!");
        return RC_OK;
    }
    free(ptr);
    //printf("page pinned done!");
}



/*====================================================================Statistics Functions=======================================================================*/

// to get content of each frame
extern PageNumber *getFrameContents(BM_BufferPool *const bm){
    // creating memory for frame
    PageNumber *frames= malloc(sizeof(PageNumber) * bufferSize);

    PgFrame *existingFrames=(PgFrame*)bm->mgmtData; // getting the frames from buffer pool

    int index=0;

    while(index <bufferSize){
        // checking whether if the frame have page
        if(existingFrames[index].pgNumber!=-1) frames[index]=existingFrames[index].pgNumber; // store the page number
        else frames[index]=NO_PAGE; // store it as no page
        index++;
    }

    return frames; // return the frames data
}

// get data on dirty flags
extern bool *getDirtyFlags(BM_BufferPool *const bm){
     // creating memory for frame
    bool *flags= malloc(sizeof(bool) * bufferSize);

    PgFrame *existingFrames=(PgFrame*)bm->mgmtData; // getting the frames from buffer pool

    int index=0;

    while(index <bufferSize){
        // checking whether if the page is dirty
        if(existingFrames[index].isDirty==TRUE) flags[index]=TRUE; // if dirty store it as true
        else flags[index]=FALSE; // if not dirty store it as false
        index++;
    }

    return flags; // return the dirty flags
}

// count of frames that are fixed for use
extern int *getFixCounts(BM_BufferPool *const bm){
    //to store the fixed frames count
    int *fixedFrames= malloc(sizeof(int) * bufferSize);

    // getting the frames from pool
    PgFrame *pageFrames=(PgFrame*) bm->mgmtData;

    int index =0;

    while(index<bufferSize){
        if(pageFrames[index].pageCounter!=-1){ // checking if the frame is fixed
            fixedFrames[index]=pageFrames[index].pageCounter; // if so, storing the count
        }
        else{
            fixedFrames[index]=0; // if not storing it as zero
        }
        index++;
    }

    return fixedFrames; // returning the fixedFrames

}

// to get number of read opeations
extern int getNumReadIO(BM_BufferPool *const bm){
    // the number of read operation is stored in diskread
    return diskRead+1; // diskread +1 is the number time data is read from disk into buffer
}

// to get number of disk write operations
extern int getNumWriteIO(BM_BufferPool *const bm){
    return diskWritten; // diskWritten has the number of time data is written from buffer into disk
}