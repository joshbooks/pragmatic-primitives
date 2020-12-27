package com.doordash.pragmatic_primitives.test_fixtures

import com.doordash.pragmatic_primitives.DataRecord
import java.util.concurrent.atomic.AtomicReferenceArray

@ExperimentalStdlibApi
object DataRecordFixtures {

    class MutableInt(val heldValue: Int) : DataRecord.MutableFieldsHolder() {
        override fun copy(): DataRecord.MutableFieldsHolder {
            return MutableInt(heldValue)
        }
    }

    class ImmutableInts(val heldValues: List<Int>) : DataRecord.ImmutableFieldsHolder()

    fun provideBasicDataRecord(): DataRecord {
        return DataRecord(
            mutableFields = AtomicReferenceArray(Array(10) {MutableInt(it)}),
            immutableFields = ImmutableInts((10..20).toList())
        )
    }
}