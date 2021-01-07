package org.joshdb.pragmatic_primitives.helper

import kotlinx.coroutines.runBlocking
import org.joshdb.pragmatic_primitives.DataRecord
import org.joshdb.pragmatic_primitives.test_fixtures.DataRecordFixtures.provideBasicDataRecord
import org.junit.Assert
import org.junit.Test

@ExperimentalStdlibApi
class StateGraphTests : BaseHelperTest() {

  fun casScxRecord(
      record: DataRecord, oldScxRecord: DataRecord.ScxRecord, newScxRecord: DataRecord.ScxRecord
  ): Boolean {
    return record.info.compareAndSet(oldScxRecord, newScxRecord)
  }

  @Test
  fun testSingleLlxIntersectingWithOneFinalizingScx() {
    val dataRecord = provideBasicDataRecord()
    val scxRecord =
        DataRecord.ScxRecord(
            affectedRecords = listOf(dataRecord),
            recordsToFinalize = listOf(dataRecord),
            recordToModify = null,
            fieldToModify = -1,
            newValue = null,
            oldValue = null,
            state = DataRecord.ScxRecord.ScxState.IN_PROGRESS,
            infoFields = listOf(DataRecord.defaultBarrierInfoValue))

    // simulate intersecting case
    dataRecord.marked = true

    Assert.assertTrue(casScxRecord(dataRecord, DataRecord.defaultBarrierInfoValue, scxRecord))

    val llxRecord =
        runBlocking { DataRecord.executeLinkedOperations { helper.loadLinkExtended(dataRecord) } }

    Assert.assertTrue(llxRecord is DataRecord.LoadLinkResult.Finalized)
    Assert.assertTrue(scxRecord.allFrozen)
    Assert.assertEquals(scxRecord.state, DataRecord.ScxRecord.ScxState.COMMITTED)
  }
}
