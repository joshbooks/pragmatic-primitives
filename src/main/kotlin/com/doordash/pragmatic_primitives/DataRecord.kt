package com.doordash.pragmatic_primitives

import com.doordash.pragmatic_primitives.exceptions.NakedOperationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import com.doordash.pragmatic_primitives.exceptions.UnlinkedScxException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicReferenceArray
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

/**
 * todo nice summary of high level ideas
 * @copyright Doordash Inc 2020
 */
@ExperimentalStdlibApi
class DataRecord(
    public val mutableFields: AtomicReferenceArray<MutableFieldsHolder>,
    public val immutableFields: ImmutableFieldsHolder
) {

    private val defaultBarrierInfoValue =
        ScxRecord(
            affectedRecords = emptyList(),
            recordsToFinalize = emptyList(),
            recordToModify = null,
            fieldToModify = -1, // if this is unset recordToModify better be too, todo doc that
            newValue = null,
            oldValue = null,
            infoFields = emptyList()
        )

    var info: AtomicReference<ScxRecord> = AtomicReference(defaultBarrierInfoValue)

    /**
     * may only change from false to true (trivial example of monotonically increasing int
     * which means we get to use a wonderfully useful specific case of the mean value theorem
     */
    // todo once again I'm not actually sure this volatile annotation is required, need to think on it more
    @Volatile
    var marked = false

    /**
     * must have a copy function that returns a shallow copy of this class
     * (ie if they contain a data record that can just be a reference to a data record, it needn't be a copy of that record)
     * (hopefully data classes just naturally override this?)
     **/
    abstract class MutableFieldsHolder {
        abstract fun copy(): MutableFieldsHolder
    }

    /**
     * all fields in this class must be immutable (vals not vars)
     *
     */
    abstract class ImmutableFieldsHolder

    /**
     * @param affectedRecords must be specified according to a partial
     * ordering that is fully explained in the docs for this class
     * @param recordsToFinalize must be a subsequence of affectedRecords
     */
    class ScxRecord(
        val affectedRecords: List<DataRecord>,
        val recordsToFinalize: List<DataRecord>,
        val recordToModify: DataRecord?,
        val fieldToModify: Int,
        val newValue: MutableFieldsHolder?,
        val oldValue: MutableFieldsHolder?,
        // TODO I'm not sure these volatile annotations are strictly required.
        // IDK I feel like they might be? But it's a hell of a performance hit to accept based on a feeling
        //
        @Volatile var state: ScxState = ScxState.COMMITTED,
        @Volatile var allFrozen: Boolean = false,
        val infoFields: List<ScxRecord>
    ) {
        /**
         * this right here is the twist on the cooperative multithreading that I think is kinda neat
         * the idea is that for a given SCX record you don't necessarily need to do all the work yourself
         * you can just do your share and suspend until your buddies helping you out finish up with their
         * share of the work, does this handle priority inversion particularly well? no, but since these are
         * coroutines rather than actual threads, preemption is less of a concern
         * The basic idea is that this number should be monotonically increasing so long as state==IN_PROGRESS
         * so we should be able to divvy the remaining work up equally between all the threads helping
         * well probably the "first" 1 << (mostSignificantBit(threadsHelping) - 1) helping to make the math easy.
         * "first" is in sarcastic quotes because it's easier to explain it that way, but when
         * mostSignificantBit(threadsHelping) changes we want more threads to start pulling their weight,
         * so I think the idea should be that those threads past the mostSignificantBit cutoff should sit in
         * a timed suspend state so that they come out of their reverie when they are needed
         */
        // todo might make more sense to use a jctools ConcurrentAutoTable here and then use the estimate_get()
        // in the tooManyCooksInTheKitchen loop
        val threadsHelping = AtomicInteger(0)

        enum class ScxState {
            IN_PROGRESS,
            COMMITTED,
            ABORTED
        }
    }

    class LlxRecord(
        val infoField: ScxRecord,
        val snapshotOfMutableFields: Array<MutableFieldsHolder>
    )

    sealed class LoadLinkResult {
        object Fail : LoadLinkResult()
        object Finalized: LoadLinkResult()
        data class Success<M4>(val snapshot: M4): LoadLinkResult()
    }


    class LocalLlxTableElement(
        val llxRecords: MutableMap<DataRecord, LlxRecord> = mutableMapOf()
    ) : CoroutineContext.Element {
        companion object Key: CoroutineContext.Key<LocalLlxTableElement>

        override val key: CoroutineContext.Key<LocalLlxTableElement>
            get() = LocalLlxTableElement
    }

    companion object {

        /**
         * word of caution, the asymptotic performance is O(n) where n is the number of LLXs
         * performed by the given coroutine context, so you probably want start a coroutine context
         * for every logical division of LLX, VLX, and SCX operations, to avoid iterating through
         * LlxRecords that you know will no longer be required for further VLX or SCX operations
         * for the forseeable future (LLX is not super expensive, better to err on the side of reloading
         * rather than accumulating a bunch of LlxRecords in your coroutine context)
         */
        suspend inline fun loadLinkExtended(record: DataRecord): LoadLinkResult {
            val marked1 = record.marked
            val recordInfo = record.info.get()
            val state = recordInfo.state
            // special case of the mean value theorem for monotonically increasing values
            // gets super cool right here
            val marked2 = record.marked


            if (
                state == ScxRecord.ScxState.ABORTED || (state == ScxRecord.ScxState.COMMITTED && !marked2)
            ) {
                val localMutableFields =
                    AtomicReferenceArray(
                        Array(record.mutableFields.length()) { idx -> record.mutableFields.get(idx) } )

                if (record.info.get() === recordInfo) {
                    (
                        coroutineContext[LocalLlxTableElement] ?: throw NakedOperationException()
                        ).llxRecords[record] =
                        LlxRecord(recordInfo, Array(localMutableFields.length()) { localMutableFields.get(it) })

                    return LoadLinkResult.Success(localMutableFields)
                }
            }

            return if (
                (
                    recordInfo.state == ScxRecord.ScxState.COMMITTED ||
                        (recordInfo.state == ScxRecord.ScxState.IN_PROGRESS && originalHelp(recordInfo))
                    ) &&
                marked1
            ) {
                LoadLinkResult.Finalized
            } else {
                LoadLinkResult.Fail
            }
        }

        private fun getNewTableContext(): CoroutineContext {
            return LocalLlxTableElement()
        }

        private suspend fun appropriateTableContents(): CoroutineContext {
            return coroutineContext[LocalLlxTableElement]?.let {
                return it
            } ?: getNewTableContext()
        }

        public suspend fun<T> executeLinkedOperations(block: suspend() -> T): T {
            return withContext(appropriateTableContents()) {block()}
        }

        fun tooManyCooksInTheKitchen(myHelperNumber: Int, threadsHelping: AtomicInteger): Boolean {
            return myHelperNumber > threadsHelping.get().takeHighestOneBit()
        }

        /**
         * for the first iteration you should specify -myHelperNumber (where myHelperNumber is 1-based) as prevIndex
         */
        private fun nextIndex(prevIndex: Int, numThreadsHelping: Int): Int {
            // idk this might actually be right? update after thinking: it definitely is not, however there's another
            // simple-ish function that should do the trick
            // todo rigorously determine this works in all cases
            return prevIndex + numThreadsHelping
        }

        fun originalHelp(
            scxRecord: ScxRecord
        ): Boolean {

            (scxRecord.affectedRecords.indices).forEach { idx ->
                // not the most descriptive variable names, copying from Trevor Brown's pseudocode
                // for a different type of clarity
                // yes, there's an unsafe cast here, I am operating under the assumption that someone
                // using a synchronized primitive more complicated than CAS is a grownup
                // and doesn't need me to hold their hand
                val rinfo = scxRecord.infoFields.getOrNull(idx) as ScxRecord?
                val r = scxRecord.affectedRecords[idx]

                if (!r.info.compareAndSet(rinfo, scxRecord)) {
                    if (r.info.get() !== scxRecord) {
                        // lifting out return here would be weird because of the state change done in the else branch
                        // makes this logic look more functional than it actually is
                        @Suppress("LiftReturnOrAssignment")
                        if (scxRecord.allFrozen) {
                            return true
                        } else {
                            scxRecord.state = ScxRecord.ScxState.ABORTED
                            return false
                        }
                    }
                }
            }

            scxRecord.allFrozen = true

            scxRecord.recordsToFinalize.forEach { record -> record.marked = true }

            scxRecord.recordToModify?.mutableFields?.compareAndExchange(scxRecord.fieldToModify, scxRecord.oldValue, scxRecord.newValue)

            scxRecord.state = ScxRecord.ScxState.COMMITTED
            return true
        }


        suspend fun help(
            scxRecord: ScxRecord
        ): Boolean {
            // todo this is the meat of the function and also the place I think I
            //  have a fun twist on the cooperative multithreading approach
            //  described in the paper that I believe adds a fairly significant progress property

            // super early exit to cut down on unnecessary administration overhead
            if (scxRecord.allFrozen) {
                return true
            }

            val myHelperNumber = scxRecord.threadsHelping.getAndIncrement()
            try {

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

                    idx = nextIndex(idx, scxRecord.threadsHelping.get())
                }


                // todo actual return values
                return false

            } finally {
                scxRecord.threadsHelping.decrementAndGet()
            }
        }

        /**
         *
         */
        suspend fun storeConditionalExtended(
            affectedRecords: List<DataRecord>,
            recordsToFinalize: List<DataRecord>,
            recordToModify: DataRecord,
            fieldToModify: Int,
            newValue: MutableFieldsHolder
        ): Boolean {
            val llxRecordsTable = coroutineContext[LocalLlxTableElement]

            val infoFields = affectedRecords.map {
                llxRecordsTable?.llxRecords?.get(it)?.infoField ?: throw UnlinkedScxException()
            }
            val oldFieldToModifyValue =
                llxRecordsTable?.get(LocalLlxTableElement)?.llxRecords?.get(recordToModify)?.let {
                    @Suppress("UNCHECKED_CAST")
                    it.snapshotOfMutableFields[fieldToModify]

                } ?: throw UnlinkedScxException()

            return originalHelp(
                ScxRecord(
                    affectedRecords = affectedRecords,
                    recordsToFinalize = recordsToFinalize,
                    recordToModify = recordToModify,
                    fieldToModify = fieldToModify,
                    newValue = newValue,
                    oldValue = oldFieldToModifyValue,
                    infoFields = infoFields
                )
            )
        }
    }
}