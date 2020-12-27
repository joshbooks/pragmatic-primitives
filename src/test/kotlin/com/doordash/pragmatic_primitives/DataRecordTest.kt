package com.doordash.pragmatic_primitives

import com.doordash.pragmatic_primitives.DataRecord.Companion.executeLinkedOperations
import com.doordash.pragmatic_primitives.DataRecord.Companion.storeConditionalExtended
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.junit.Test
import java.util.concurrent.atomic.AtomicReferenceArray

@ExperimentalStdlibApi
class DataRecordTest {

    class MutableInt(val heldValue: Int) : DataRecord.MutableFieldsHolder() {
        override fun copy(): DataRecord.MutableFieldsHolder {
            return MutableInt(heldValue)
        }
    }

    class ImmutableInts(val heldValues: List<Int>) : DataRecord.ImmutableFieldsHolder()


    @Test
    fun testThatLLXWorksAtAll() {
        val testRecord =
            DataRecord(
                mutableFields = AtomicReferenceArray(Array(10) {MutableInt(it)}),
                immutableFields = ImmutableInts((10..20).toList())
            )

        val llResult =
            runBlocking {
                executeLinkedOperations {
                    DataRecord.loadLinkExtended(testRecord)
                }
            }

        println(llResult)
        Assert.assertTrue(llResult is DataRecord.LoadLinkResult.Success<*>)
    }

    @Test
    fun testThatSCXWorksAtAll() {
        val testRecord =
            DataRecord(
                mutableFields = AtomicReferenceArray(Array(10) {MutableInt(it)}),
                immutableFields = ImmutableInts((10..20).toList())
            )

        val llResult =
            runBlocking {
                executeLinkedOperations {
                    DataRecord.loadLinkExtended(testRecord)

                    storeConditionalExtended(
                        listOf(testRecord),
                        listOf(),
                        testRecord,
                        1,
                        MutableInt(1337)
                    )
                    DataRecord.loadLinkExtended(testRecord)
                }
            }

        println(llResult)
        Assert.assertTrue(llResult is DataRecord.LoadLinkResult.Success<*>)
        val typedResult = llResult as DataRecord.LoadLinkResult.Success<AtomicReferenceArray<MutableInt>>

        Assert.assertEquals(typedResult.snapshot.get(1).heldValue, 1337)
    }
}