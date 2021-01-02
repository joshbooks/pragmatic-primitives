package com.doordash.pragmatic_primitives.helper

import com.doordash.pragmatic_primitives.DataRecord
import com.doordash.pragmatic_primitives.exceptions.NakedOperationException
import com.doordash.pragmatic_primitives.exceptions.UnlinkedScxException
import java.util.concurrent.atomic.AtomicReferenceArray
import kotlin.coroutines.coroutineContext

@ExperimentalStdlibApi
abstract class BaseHelper {
    abstract suspend fun help(scxRecord: DataRecord.ScxRecord): Boolean

    /**
     * Performs an LLX operation. Fails with a NakedOperationException if performed in a coroutine context that
     * doesn't have the LocalLlxTableElement key initialized.
     *
     * word of caution, the asymptotic performance is O(n) where n is the number of LLXs
     * performed by the given coroutine context, so you probably want start a coroutine context
     * for every logical division of LLX, VLX, and SCX operations, to avoid iterating through
     * LlxRecords that you know will no longer be required for further VLX or SCX operations
     * for the forseeable future (LLX is not super expensive, better to err on the side of reloading
     * rather than accumulating a bunch of LlxRecords in your coroutine context)
     */
    suspend inline fun loadLinkExtended(record: DataRecord): DataRecord.LoadLinkResult {
        val marked1 = record.marked
        val recordInfo = record.info.get()
        val state = recordInfo.state
        // special case of the mean value theorem for monotonically increasing values
        // gets super cool right here
        val marked2 = record.marked


        if (
            state == DataRecord.ScxRecord.ScxState.ABORTED || (state == DataRecord.ScxRecord.ScxState.COMMITTED && !marked2)
        ) {
            val localMutableFields =
                AtomicReferenceArray(
                    Array(record.mutableFields.length()) { idx -> record.mutableFields.get(idx) })

            if (record.info.get() === recordInfo) {
                (
                    coroutineContext[DataRecord.LocalLlxTableElement] ?: throw NakedOperationException()
                    ).llxRecords[record] =
                    DataRecord.LlxRecord(
                        recordInfo,
                        Array(localMutableFields.length()) { localMutableFields.get(it) })

                return DataRecord.LoadLinkResult.Success(
                    localMutableFields
                )
            }
        }

        return if (
            (
                recordInfo.state == DataRecord.ScxRecord.ScxState.COMMITTED ||
                    // this second case seems like it allows an LLX to return Finalized before the
                    // modification of fld in the Scx record that was InProgress for this data record is complete
                    // that seems like a somewhat significant gotcha for other uses of Finalized besides marking
                    // nodes as stale
                    // todo make sure that is the case and that's kosher
                    (recordInfo.state == DataRecord.ScxRecord.ScxState.IN_PROGRESS && help(recordInfo))
                ) &&
            marked1
        ) {
            DataRecord.LoadLinkResult.Finalized
        } else {
            DataRecord.LoadLinkResult.Fail
        }
    }

    /**
     * Performs an SCX operation. Fails with a NakedOperationException if performed in a coroutine context that
     * doesn't have the LocalLlxTableElement key initialized. Fails with an UnlinkedScxException if any of the
     * DataRecords specified were not linked by a prior LLX operation in this "thread" (ie do not have an associated
     * record in the LocalLlxTableElement record in the coroutine context this operation is executed in) or if
     * the previously performed LLX operation did not return Success.
     *
     * @param affectedRecords Records on which an LLX operation has previously been executed upon which the
     * execution of this SCX operation should depend
     * @param recordsToFinalize records that should be finalized as part of this SCX operation
     * @param recordToModify the single record that should be modified as part of this SCX record, should not
     * be set if fieldToModify is set to a negative value
     * @param fieldToModify the array index of the mutable field to modify in the DataRecord specified, if set to a
     * negative value no modification will be performed
     * @param newValue the value to which the recordToModify.mutableFields.get(fieldToModify) should be set
     */
    suspend fun storeConditionalExtended(
        affectedRecords: List<DataRecord>,
        recordsToFinalize: List<DataRecord>,
        recordToModify: DataRecord,
        fieldToModify: Int,
        newValue: DataRecord.MutableFieldsHolder
    ): Boolean {
        val llxRecordsTable = coroutineContext[DataRecord.LocalLlxTableElement]

        val infoFields = affectedRecords.map {
            llxRecordsTable?.llxRecords?.get(it)?.infoField ?: throw UnlinkedScxException()
        }
        val oldFieldToModifyValue =
            llxRecordsTable?.get(DataRecord.LocalLlxTableElement)?.llxRecords?.get(recordToModify)?.let {
                @Suppress("UNCHECKED_CAST")
                it.snapshotOfMutableFields[fieldToModify]

            } ?: throw UnlinkedScxException()

        return help(
            DataRecord.ScxRecord(
                affectedRecords = affectedRecords,
                recordsToFinalize = recordsToFinalize,
                recordToModify = recordToModify,
                fieldToModify = fieldToModify,
                newValue = newValue,
                oldValue = oldFieldToModifyValue,
                state = DataRecord.ScxRecord.ScxState.IN_PROGRESS,
                infoFields = infoFields
            )
        )
    }
}