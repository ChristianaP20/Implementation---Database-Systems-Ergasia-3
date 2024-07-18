#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf.h"
#include "hp_file.h"
#include "record.h"
#include "sort.h"
#include "merge.h"
#include "chunk.h"

bool shouldSwap(Record* rec1,Record* rec2){
    if (strcmp(rec1->name, rec2->name) > 0)
        return true;
    // If the names are the same, compare the surnames.
    else if (strcmp(rec1->name, rec2->name) == 0 && strcmp(rec1->surname, rec2->surname) > 0)
        return true;
    return false;
}

void sort_Chunk(CHUNK* chunk){
    int i, j;
    Record rec1, rec2;

    // Apply a sorting algorithm (bubble sort) within the chunk
    for (i = 0; i < chunk->recordsInChunk - 1; i++) {
        for (j = 0; j < chunk->recordsInChunk - i - 1; j++) {
            if (CHUNK_GetIthRecordInChunk(chunk, j, &rec1) == 0 &&
                CHUNK_GetIthRecordInChunk(chunk, j + 1, &rec2) == 0) {
                if (shouldSwap(&rec1, &rec2)) {
                    CHUNK_UpdateIthRecord(chunk, j, rec2);
                    CHUNK_UpdateIthRecord(chunk, j + 1, rec1);
                }
            }
        }
    }
}


//Sorts a heap file into chunks
void sort_FileInChunks(int file_desc, int numBlocksInChunk){
    CHUNK_Iterator iterator = CHUNK_CreateIterator(file_desc, numBlocksInChunk);
    CHUNK chunk;

    while (CHUNK_GetNext(&iterator, &chunk) == 0) {
        sort_Chunk(&chunk);
    }
}
