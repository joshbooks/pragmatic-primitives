package com.doordash.pragmatic_primitives.exceptions

import java.lang.RuntimeException

class NakedOperationException :
    RuntimeException(
        "An LLX, SCX, or VLX operation was attempted outside a executeLinkedOperations block"
    )