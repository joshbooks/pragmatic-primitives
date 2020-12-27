package com.doordash.pragmatic_primitives.helper

import com.doordash.pragmatic_primitives.DataRecord

@ExperimentalStdlibApi
class StateGraphTests {

    fun casScxRecord(record: DataRecord, oldScxRecord: DataRecord.ScxRecord, newScxRecord: DataRecord.ScxRecord) {
        record.info.compareAndExchange(oldScxRecord, newScxRecord)
    }



}