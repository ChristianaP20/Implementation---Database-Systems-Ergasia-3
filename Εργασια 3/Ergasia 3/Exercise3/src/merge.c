#include <merge.h>
#include <stdio.h>
#include <stdbool.h>

void merge(int input_FileDesc, int chunkSize, int bWay, int output_FileDesc ){ 
    //printf("Before Creating Iterator: File Descriptor: %d\n", input_FileDesc);
    CHUNK_Iterator iterator = CHUNK_CreateIterator(input_FileDesc, chunkSize);

    // Allocate memory for CHUNK array
    CHUNK *chunks = (CHUNK *)malloc(bWay * sizeof(CHUNK));
    if (chunks == NULL) {
        fprintf(stderr, "Failed to allocate memory for chunks\n");
        return;
    }

    // Allocate memory for records to merge
    Record *recordsToMerge = (Record *)malloc(bWay * sizeof(Record));
    if (recordsToMerge == NULL) {
        fprintf(stderr, "Failed to allocate memory for recordsToMerge\n");
        free(chunks);
        return;
    }

    // Allocate memory for record iterators
    CHUNK_RecordIterator *recordIterators = (CHUNK_RecordIterator *)malloc(bWay * sizeof(CHUNK_RecordIterator));
    if (recordIterators == NULL) {
        fprintf(stderr, "Failed to allocate memory for recordIterators\n");
        free(chunks);
        free(recordsToMerge);
        return;
    }

    // Initialize record iterators for each chunk
    for (int i = 0; i < bWay; ++i) {
        if (CHUNK_GetNext(&iterator, &(chunks[i])) != 0) {
            // Handle the error, return or exit the function
            fprintf(stderr, "Error getting next chunk for iteration %d. current: %d, lastBlocksID: %d, chunk range: %d-%d\n", i, iterator.current, iterator.lastBlocksID, chunks[i].from_BlockId, chunks[i].to_BlockId);
            // Clean up and exit
            free(chunks);
            free(recordsToMerge);
            free(recordIterators);
            return;
        }

        // Fix for the chunk range for all iterations
        chunks[i].from_BlockId = iterator.current - iterator.blocksInChunk;
        chunks[i].to_BlockId = iterator.current - 1;

        recordIterators[i] = CHUNK_CreateRecordIterator(&(chunks[i]));
        fprintf(stderr, "Iteration %d: current: %d, chunk range: %d-%d\n", i, iterator.current, chunks[i].from_BlockId, chunks[i].to_BlockId);
    }

    
    // Iterate through records in chunks and merge them
    while (bWay > 1) {
        Record minRecord;
        int minIndex = -1;
    
        // Iterate through record iterators and find the minimum record
        for (int i = 0; i < bWay; i++) {
            // Check if there are more records in the current chunk
            if (CHUNK_GetNextRecord(&recordIterators[i], &(recordsToMerge[i])) == 1) {
                // Update the minimum record if needed
                if (minIndex == -1 || shouldSwap(&(recordsToMerge[i]), &minRecord)) {
                    minRecord = recordsToMerge[i];
                    minIndex = i;
                }
            }
        }
        
        if (minIndex == -1) {
            // No more records to merge, break the loop
            break;
        }
        
        // Write the minimum record to the output file
        HP_InsertEntry(output_FileDesc, minRecord);
        
        // Check if the current chunk of the minimum record is exhausted
        if (CHUNK_GetNextRecord(&(recordIterators[minIndex]), &(recordsToMerge[minIndex])) != 1) {
            // Get the next chunk for the exhausted iterator
            if (CHUNK_GetNext(&iterator, &(recordIterators[minIndex].chunk)) == 1) {
                recordIterators[minIndex] = CHUNK_CreateRecordIterator(&(recordIterators[minIndex].chunk));
            }
            // An update to the current record from the corresponding chunk
            else if (CHUNK_GetNextRecord(&recordIterators[minIndex], &(recordsToMerge[minIndex])) == -1) {
                // If there is no more record, remove the chunk from the merge
                for (int i = minIndex; i < bWay - 1; i++) {
                    chunks[i] = chunks[i + 1];
                    //iterator[i] = iterator[i + 1];
                    recordIterators[i] = recordIterators[i + 1];
                }
                bWay--;

                // If there are no more chunks, terminate
                if (bWay == 0) {
                    break;
                }
            }
        }
    }

    if (iterator.current <= iterator.lastBlocksID) {
        iterator = CHUNK_CreateIterator(input_FileDesc, chunkSize);
    }


    free(chunks);  // Απελευθέρωση μνήμης
    free(recordsToMerge);
    free(recordIterators);
}
