package com.doordash.pragmatic_primitives.exceptions

import java.lang.RuntimeException

class UnlinkedScxException :
    RuntimeException(
        "SCX attempted on record not linked to a prior LLX in this coroutine context"
    )