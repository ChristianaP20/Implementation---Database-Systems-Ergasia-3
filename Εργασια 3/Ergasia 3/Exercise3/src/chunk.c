#include <merge.h>
#include <stdio.h>
#include "chunk.h"


CHUNK_Iterator CHUNK_CreateIterator(int fileDesc, int blocksInChunk){
    CHUNK_Iterator iterator;
    iterator.file_desc = fileDesc;
    iterator.current = 1;  // Starting from block 1
    //iterator.lastBlocksID = -1;  // Initializing last block id
    iterator.blocksInChunk = blocksInChunk;

    // Calculate the last valid block ID
    int error = BF_GetBlockCounter(iterator.file_desc, &(iterator.lastBlocksID));
    if (error != BF_OK || iterator.lastBlocksID < 0) {
        // Handle error
        fprintf(stderr, "Error getting block counter or invalid lastBlocksID. Error code: %d\n", error);
        iterator.lastBlocksID = 0;  // Set a default value
    } else {
        // Increment the lastBlocksID to represent the last valid block ID
        iterator.lastBlocksID--;
    }


    return iterator;
}

int CHUNK_GetNext(CHUNK_Iterator *iterator,CHUNK* chunk){
    if (iterator->current > iterator-> lastBlocksID) {
        // We have reached the end of the file
        //printf("End of file\n");
        return -1;
    }
    
    // Check if we are reaching the last iteration
    if (iterator->current + iterator->blocksInChunk > iterator->lastBlocksID) {
        // Adjust to the last valid block in the file
        iterator->blocksInChunk = iterator->lastBlocksID - iterator->current + 1;
    }

    chunk->file_desc = iterator->file_desc;
    chunk->from_BlockId = iterator->current; //assuming block numbering starts from 0
    chunk->to_BlockId = iterator->current + iterator->blocksInChunk - 1;
    chunk->recordsInChunk = 0;
    chunk->blocksInChunk = iterator->blocksInChunk;

    // Adjust to the last valid block in the file
    if (chunk->to_BlockId > iterator->lastBlocksID) {
        chunk->to_BlockId = iterator->lastBlocksID;
    }

    // Move to the next iteration
    iterator->current += iterator->blocksInChunk;

    //printf("Debug: from_BlockId: %d, to_BlockId: %d\n", chunk->from_BlockId, chunk->to_BlockId);
    //printf("Debug: File Desc: %d, From Block: %d, To Block: %d\n", chunk->file_desc, chunk->from_BlockId, chunk->to_BlockId);


    return 0; // Successfully retrieved the next CHUNK
}

int CHUNK_GetIthRecordInChunk(CHUNK* chunk,  int i, Record* record){
    // Check if i is within the valid range of records in the chunk
    if (i < 0 || i >= chunk->recordsInChunk) {
        return -1; // i is out of bounds
    }

    //Calculate the block and record index within the chunk
    int blockIndex = i / HP_GetMaxRecordsInBlock(chunk->file_desc);
    int cursor = i % HP_GetMaxRecordsInBlock(chunk->file_desc);

    int blockId = chunk->from_BlockId + blockIndex;

    if (HP_GetRecord(chunk->file_desc, blockId, cursor, record) == -1) {
        printf("Error\n");
        return -1; // Failed to retrieve record
    }

    HP_Unpin(chunk->file_desc, blockId);
    return 0; // Successfully retrieved the ith record
}

int CHUNK_UpdateIthRecord(CHUNK* chunk,  int i, Record record){
    // Check if i is within the valid range of records in the chunk
    if (i < 0 || i >= chunk->recordsInChunk) {
        return -1; // i is out of bounds
    }

    //Calculate the block and record index within the chunk
    int blockIndex = i / HP_GetMaxRecordsInBlock(chunk->file_desc);
    int cursor = i % HP_GetMaxRecordsInBlock(chunk->file_desc);

    int blockId = chunk->from_BlockId + blockIndex;

    if (HP_UpdateRecord(chunk->file_desc, blockId, cursor, record) == -1) {
        printf("ERROR\n");
        return -1; // Failed to update record
    }

    HP_Unpin(chunk->file_desc, blockId);
    return 0; // Successfully updated the ith record
}

void CHUNK_Print(CHUNK chunk){
    printf("Chunk from Block %d to Block %d\n", chunk.from_BlockId, chunk.to_BlockId);

    Record record;
    for (int i = 0; i < chunk.recordsInChunk; ++i) {
        if (CHUNK_GetIthRecordInChunk(&chunk, i, &record) == 0) {
            printf("Record %d:\n", i +1);
            printf("Record %d: %s, %s, %s, %s\n", record.id, record.name, record.surname, record.city, record.delimiter);
        } else {
            printf("Error: Unable to retrieve record %d from the chunk.\n", i + 1);
        }
    }
}


CHUNK_RecordIterator CHUNK_CreateRecordIterator(CHUNK *chunk){
    CHUNK_RecordIterator iterator;
    iterator.chunk = *chunk;
    iterator.currentBlockId = chunk->from_BlockId;
    iterator.cursor = 0;
    return iterator;
}

int CHUNK_GetNextRecord(CHUNK_RecordIterator *iterator,Record* record){
    if (iterator->currentBlockId > iterator->chunk.to_BlockId) {
        // Iterator has reached the end of the CHUNK
        return 0;
    }   

    int result = HP_GetRecord(iterator->chunk.file_desc, iterator->currentBlockId, iterator->cursor, record);

    iterator->cursor++;
    if (iterator->cursor >= HP_GetMaxRecordsInBlock(iterator->chunk.file_desc)) {
        // We have reached the end of the block, move to the next block
        iterator->currentBlockId++;
        iterator->cursor = 0;
    }

    HP_Unpin(iterator->chunk.file_desc, iterator->currentBlockId);
    return result;
}
