package com.doordash.pragmatic_primitives.helper

import com.doordash.pragmatic_primitives.DataRecord
import com.doordash.pragmatic_primitives.DataRecord.Companion.executeLinkedOperations
import com.doordash.pragmatic_primitives.exceptions.UnlinkedScxException
import com.doordash.pragmatic_primitives.test_fixtures.DataRecordFixtures
import com.doordash.pragmatic_primitives.test_fixtures.DataRecordFixtures.provideBasicDataRecord
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.junit.Test
import java.util.concurrent.atomic.AtomicReferenceArray

@ExperimentalStdlibApi
class BasicFunctionalityTest {


    private val helpers = listOf(OriginalHelper /*, WorkStealingHelper */)

    @Test
    fun testThatLLXWorksAtAll() {
        helpers.forEach { testThatLLXWorksAtAllWithHelper(it) }
    }

    private fun testThatLLXWorksAtAllWithHelper(helper: BaseHelper) {
        val testRecord = provideBasicDataRecord()

        val llResult =
            runBlocking {
                executeLinkedOperations {
                    helper.loadLinkExtended(testRecord)
                }
            }

        println(llResult)
        Assert.assertTrue(llResult is DataRecord.LoadLinkResult.Success<*>)
        val typedResult =
            llResult as DataRecord.LoadLinkResult.Success<AtomicReferenceArray<DataRecordFixtures.MutableInt>>

        Assert.assertEquals(typedResult.snapshot.get(1).heldValue, 1)
    }

    @Test
    fun testThatSCXWorksAtAll() {
        helpers.forEach { testThatSCXWorksAtAllWithHelper(it) }
    }

    private fun testThatSCXWorksAtAllWithHelper(helper: BaseHelper) {
        val testRecord = provideBasicDataRecord()

        val llResult =
            runBlocking {
                executeLinkedOperations {
                    helper.loadLinkExtended(testRecord)

                    helper.storeConditionalExtended(
                        listOf(testRecord),
                        listOf(),
                        testRecord,
                        1,
                        DataRecordFixtures.MutableInt(1337)
                    )
                    helper.loadLinkExtended(testRecord)
                }
            }

        println(llResult)
        Assert.assertTrue(llResult is DataRecord.LoadLinkResult.Success<*>)
        val typedResult = llResult as DataRecord.LoadLinkResult.Success<AtomicReferenceArray<DataRecordFixtures.MutableInt>>

        Assert.assertEquals(typedResult.snapshot.get(1).heldValue, 1337)
    }

    @Test
    fun testThatSCXFinalizeWorksAtAll() {
        helpers.forEach { testThatSCXFinalizeWorksAtAllWithHelper(it) }
    }

    private fun testThatSCXFinalizeWorksAtAllWithHelper(helper: BaseHelper) {

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
                        DataRecordFixtures.MutableInt(1337)
                    )
                    helper.loadLinkExtended(testRecord)
                }
            }

        println(llResult)
        Assert.assertTrue(llResult is DataRecord.LoadLinkResult.Finalized)
    }

    @Test
    fun testThatSCXFailsAfterFinalizedLLX() {
        helpers.forEach { testThatSCXFailsAfterFinalizedLLXWithHelper(it) }
    }

    fun testThatSCXFailsAfterFinalizedLLXWithHelper(helper: BaseHelper) {
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
                        DataRecordFixtures.MutableInt(1337)
                    )
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
                            listOf(testRecord),
                            listOf(),
                            testRecord,
                            1,
                            DataRecordFixtures.MutableInt(31337)
                        )
                    }
                }
            }

        Assert.assertTrue(scxException.exceptionOrNull() is UnlinkedScxException)
    }
}