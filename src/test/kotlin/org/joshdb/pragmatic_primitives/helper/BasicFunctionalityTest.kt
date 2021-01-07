package org.joshdb.pragmatic_primitives.helper

import java.util.concurrent.atomic.AtomicReferenceArray
import kotlinx.coroutines.runBlocking
import org.joshdb.pragmatic_primitives.DataRecord
import org.joshdb.pragmatic_primitives.DataRecord.Companion.executeLinkedOperations
import org.joshdb.pragmatic_primitives.exceptions.UnlinkedScxException
import org.joshdb.pragmatic_primitives.test_fixtures.DataRecordFixtures
import org.joshdb.pragmatic_primitives.test_fixtures.DataRecordFixtures.provideBasicDataRecord
import org.junit.Assert
import org.junit.Test

@ExperimentalStdlibApi
class BasicFunctionalityTest : BaseHelperTest() {

  @Test
  fun testThatLLXWorksAtAll() {
    val testRecord = provideBasicDataRecord()

    val llResult = runBlocking { executeLinkedOperations { helper.loadLinkExtended(testRecord) } }

    println(llResult)
    Assert.assertTrue(llResult is DataRecord.LoadLinkResult.Success<*>)
    val typedResult =
        llResult as DataRecord.LoadLinkResult.Success<
            AtomicReferenceArray<DataRecordFixtures.MutableInt>>

    Assert.assertEquals(typedResult.snapshot.get(1).heldValue, 1)
  }

  @Test
  fun testThatSCXWorksAtAll() {
    val testRecord = provideBasicDataRecord()

    val llResult =
        runBlocking {
          executeLinkedOperations {
            helper.loadLinkExtended(testRecord)

            helper.storeConditionalExtended(
                listOf(testRecord), listOf(), testRecord, 1, DataRecordFixtures.MutableInt(1337))
            helper.loadLinkExtended(testRecord)
          }
        }

    println(llResult)
    Assert.assertTrue(llResult is DataRecord.LoadLinkResult.Success<*>)
    val typedResult =
        llResult as DataRecord.LoadLinkResult.Success<
            AtomicReferenceArray<DataRecordFixtures.MutableInt>>

    Assert.assertEquals(typedResult.snapshot.get(1).heldValue, 1337)
  }

  @Test
  fun testThatSCXFinalizeWorksAtAll() {
    val testRecord = provideBasicDataRecord()

    val llResult =
        runBlocking {
          executeLinkedOperations {
            helper.loadLinkExtended(testRecord)

            helper.storeConditionalExtended(
                listOf(testRecord),
                listOf(testRecord),
                testRecord,
                1,
                DataRecordFixtures.MutableInt(1337))
            helper.loadLinkExtended(testRecord)
          }
        }

    println(llResult)
    Assert.assertTrue(llResult is DataRecord.LoadLinkResult.Finalized)
  }

  @Test
  fun testThatSCXFailsAfterFinalizedLLX() {
    val testRecord = provideBasicDataRecord()

    val llResult =
        runBlocking {
          executeLinkedOperations {
            helper.loadLinkExtended(testRecord)

            helper.storeConditionalExtended(
                listOf(testRecord),
                listOf(testRecord),
                testRecord,
                1,
                DataRecordFixtures.MutableInt(1337))
            helper.loadLinkExtended(testRecord)
          }
        }

    println(llResult)
    Assert.assertTrue(llResult is DataRecord.LoadLinkResult.Finalized)

    val scxException =
        kotlin.runCatching {
          runBlocking {
            executeLinkedOperations {
              helper.loadLinkExtended(testRecord)

              helper.storeConditionalExtended(
                  listOf(testRecord), listOf(), testRecord, 1, DataRecordFixtures.MutableInt(31337))
            }
          }
        }

    Assert.assertTrue(scxException.exceptionOrNull() is UnlinkedScxException)
  }
}
