package org.joshdb.pragmatic_primitives.helper

import org.joshdb.pragmatic_primitives.DataRecord

@ExperimentalStdlibApi
object OriginalHelper : BaseHelper() {
  override suspend fun help(scxRecord: DataRecord.ScxRecord): Boolean {

    (scxRecord.affectedRecords.indices).forEach { idx ->
      // not the most descriptive variable names, copying from Trevor Brown's pseudocode
      // for a different type of clarity
      val rinfo = scxRecord.infoFields.getOrNull(idx)
      val r = scxRecord.affectedRecords[idx]

      if (!r.info.compareAndSet(rinfo, scxRecord)) {
        if (r.info.get() !== scxRecord) {
          // lifting out return here would be weird because of the state change done in the else
          // branch
          // makes this logic look more functional than it actually is
          @Suppress("LiftReturnOrAssignment")
          if (scxRecord.allFrozen) {
            return true
          } else {
            scxRecord.state = DataRecord.ScxRecord.ScxState.ABORTED
            return false
          }
        }
      }
    }

    scxRecord.allFrozen = true

    scxRecord.recordsToFinalize.forEach { record -> record.marked = true }

    scxRecord.recordToModify
        ?.mutableFields
        ?.compareAndExchange(scxRecord.fieldToModify, scxRecord.oldValue, scxRecord.newValue)

    scxRecord.state = DataRecord.ScxRecord.ScxState.COMMITTED
    return true
  }
}
