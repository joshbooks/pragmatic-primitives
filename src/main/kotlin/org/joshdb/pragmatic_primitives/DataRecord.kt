package org.joshdb.pragmatic_primitives

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicReferenceArray
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.withContext

/**
 * Class that provides the non-blocking synchronized operation described in the paper Pragmatic
 * Primitives for Non-blocking Data Structures by Trevor Brown, Faith Ellen, and Eric Ruppert
 *
 * Also I had an idea for a little twist on the helper function to improve the asymptotic
 * performance of the helper function by distributing the state writing across threads rather than
 * duplicating effort.
 *
 * The high level idea of these operations is that you do some number of LLXs on Data Records,
 * optionally VLXs on some subset of those records, then optionally an SCX on some subset of those
 * records, finalizing some number of them and modifying one of them; the SCX will only succeed if
 * those Data Records have not been changed or finalized in between the LLX(s) you did and the SCX.
 * The utility of this being that when you make non-blocking data structures you often want to
 * change a record in a way that invalidates some number of "downstream" records and it's necessary
 * that the change and the invalidation of the associated "downstream" records be atomic with
 * respect to other "threads".
 *
 * This implementation is in Kotlin and makes use of Kotlin coroutines, so the "threads" are in fact
 * user mode threads rather than OS threads. To support this mode of execution the supporting thread
 * level tables are stored in the coroutine context, to initialize these you can use the helper
 * function executeLinkedOperations to execute blocks of LLX, VLX, SCX operations.
 *
 * @copyright Doordash Inc 2020
 * @author Joshua Hight
 */
@ExperimentalStdlibApi
class DataRecord(
    public val mutableFields: AtomicReferenceArray<MutableFieldsHolder>,
    public val immutableFields: ImmutableFieldsHolder
) {

  var info: AtomicReference<ScxRecord> = AtomicReference(defaultBarrierInfoValue)

  /**
   * may only change from false to true (trivial example of monotonically increasing int which means
   * we get to use a wonderfully useful specific case of the mean value theorem
   */
  // todo once again I'm not actually sure this volatile annotation is required, need to think on it
  // more
  @Volatile var marked = false

  /**
   * must have a copy function that returns a shallow copy of this class (ie if they contain a data
   * record that can just be a reference to a data record, it needn't be a copy of that record)
   * (hopefully data classes just naturally override this?)
   */
  abstract class MutableFieldsHolder {
    abstract fun copy(): MutableFieldsHolder
  }

  /** all fields in this class must be immutable (vals not vars) */
  abstract class ImmutableFieldsHolder

  /**
   * @param affectedRecords must be specified according to a partial ordering that is fully
   * explained in the docs for this class
   * @param recordsToFinalize must be a subsequence of affectedRecords
   */
  class ScxRecord(
      val affectedRecords: List<DataRecord>,
      val recordsToFinalize: List<DataRecord>,
      val recordToModify: DataRecord?,
      val fieldToModify: Int,
      val newValue: MutableFieldsHolder?,
      val oldValue: MutableFieldsHolder?,
      // TODO I'm not sure these volatile annotations are strictly required.
      // IDK I feel like they might be? But it's a hell of a performance hit to accept based on a
      // feeling
      //
      @Volatile var state: ScxState = ScxState.COMMITTED,
      @Volatile var allFrozen: Boolean = false,
      val infoFields: List<ScxRecord>
  ) {
    /**
     * this right here is the twist on the cooperative multithreading that I think is kinda neat the
     * idea is that for a given SCX record you don't necessarily need to do all the work yourself
     * you can just do your share and suspend until your buddies helping you out finish up with
     * their share of the work, does this handle priority inversion particularly well? no, but since
     * these are coroutines rather than actual threads, preemption is less of a concern The basic
     * idea is that this number should be monotonically increasing so long as state==IN_PROGRESS so
     * we should be able to divvy the remaining work up equally between all the threads helping well
     * probably the "first" 1 << (mostSignificantBit(threadsHelping) - 1) helping to make the math
     * easy. "first" is in sarcastic quotes because it's easier to explain it that way, but when
     * mostSignificantBit(threadsHelping) changes we want more threads to start pulling their
     * weight, so I think the idea should be that those threads past the mostSignificantBit cutoff
     * should sit in a timed suspend state so that they come out of their reverie when they are
     * needed
     */
    // todo might make more sense to use a jctools ConcurrentAutoTable here and then use the
    // estimate_get()
    // in the tooManyCooksInTheKitchen loop
    val threadsHelping = AtomicInteger(0)

    // todo might make sense to use a jctools ConcurrentAutoTable here as well
    val threadsCompleted = AtomicInteger(0)

    enum class ScxState {
      IN_PROGRESS,
      COMMITTED,
      ABORTED
    }
  }

  class LlxRecord(
      val infoField: ScxRecord, val snapshotOfMutableFields: Array<MutableFieldsHolder>)

  sealed class LoadLinkResult {
    object Fail : LoadLinkResult()
    object Finalized : LoadLinkResult()
    data class Success<M4>(val snapshot: M4) : LoadLinkResult()
  }

  class LocalLlxTableElement(val llxRecords: MutableMap<DataRecord, LlxRecord> = mutableMapOf()) :
      CoroutineContext.Element {
    companion object Key : CoroutineContext.Key<LocalLlxTableElement>

    override val key: CoroutineContext.Key<LocalLlxTableElement>
      get() = LocalLlxTableElement
  }

  companion object {
    val defaultBarrierInfoValue =
        ScxRecord(
            affectedRecords = emptyList(),
            recordsToFinalize = emptyList(),
            recordToModify = null,
            fieldToModify = -1, // if this is unset recordToModify better be too, todo doc that
            newValue = null,
            oldValue = null,
            infoFields = emptyList())

    private fun getNewTableContext(): CoroutineContext {
      return LocalLlxTableElement()
    }

    private suspend fun appropriateTableContents(): CoroutineContext {
      return coroutineContext[LocalLlxTableElement]?.let {
        return it
      }
          ?: getNewTableContext()
    }

    public suspend fun <T> executeLinkedOperations(block: suspend () -> T): T {
      return withContext(appropriateTableContents()) { block() }
    }
  }
}
