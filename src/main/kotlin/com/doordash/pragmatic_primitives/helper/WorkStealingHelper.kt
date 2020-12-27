package com.doordash.pragmatic_primitives.helper

import com.doordash.pragmatic_primitives.DataRecord
import kotlinx.coroutines.delay
import java.util.concurrent.atomic.AtomicInteger

@ExperimentalStdlibApi
object WorkStealingHelper : BaseHelper() {

    fun tooManyCooksInTheKitchen(myHelperNumber: Int, threadsHelping: AtomicInteger): Boolean {
        return myHelperNumber > threadsHelping.get().takeHighestOneBit()
    }

    /**
     * for the first iteration you should specify -myHelperNumber (where myHelperNumber is 1-based) as prevIndex
     */
    fun nextIndex(prevIndex: Int, numThreadsHelping: Int): Int {
        // idk this might actually be right? update after thinking: it definitely is not, however there's another
        // simple-ish function that should do the trick
        // todo rigorously determine this works in all cases
        return prevIndex + numThreadsHelping
    }

    override suspend fun help(scxRecord: DataRecord.ScxRecord): Boolean {
        // todo this is the place I think I
        //  have a fun twist on the cooperative multithreading approach
        //  described in the paper that I believe adds a fairly significant progress property

        // super early exit to cut down on unnecessary administration overhead
        if (scxRecord.allFrozen) {
            return true
        }

        val myHelperNumber = scxRecord.threadsHelping.getAndIncrement()
        try {

            // todo probably worth testing whether it's better to delay here or have the excess threads just
            // duplicate some other thread's work, for non uniformly fast bus speeds that's actually probably
            // better. Can probably just mod in the nextIndex function
            while (tooManyCooksInTheKitchen(
                    myHelperNumber,
                    scxRecord.threadsHelping
                )
            ) {
                delay(1)
            }

            // todo so the idea is to have another loop outside the one to "freeze" all the affectedRecords
            // by setting their SCX record. This outer loop will be driven by a hashmap of booleans indicating
            // whether or not the given helperNumber has completed with an additional check for allFrozen,
            // the behavior of the loop will be to take the offensive by picking a random helperNumber,
            // then scanning to the right until they encounter one that isn't done yet
            // (according to the hashmap of booleans), then writing backwards until we encounter a CAS failure
            // due meeting the other thread in the middle. Then we loop back round and do it again until
            // we have finished all the work for all the threads. We determine that we're done working either
            // by consulting allFrozen or doing one read of the count (this one can be an estimate) then reading
            // all the entries in the hashmap up to that count, then another read of the count(cannot be an estimate).
            // then if the two counts are equal and all the booleans up to that count are set to true we know
            // that we can safely set allFrozen

            // freeze all data records affected by this scx to protect
            // their mutable fields while this scx is going on
            var idx = myHelperNumber * -1
            while (idx < scxRecord.affectedRecords.size) {
                // todo do stuff to affected records
                val expectedRecordInfo = scxRecord.infoFields[idx]

                val record = scxRecord.affectedRecords[idx]

                @Suppress("UNCHECKED_CAST")
                val successfulSwap = record.info.compareAndSet(expectedRecordInfo, scxRecord)

                if (!successfulSwap) {
                    if (record.info.get() !== scxRecord) {
                        // if we get here it's not a contention fail with another
                        // helper thread, it's genuine fail due to a conflict (unless I'm way behind and
                    }
                }

                idx = nextIndex(
                    idx,
                    scxRecord.threadsHelping.get()
                )
            }


            // todo actual return values
            return false

        } finally {
            scxRecord.threadsHelping.decrementAndGet()
        }    }
}