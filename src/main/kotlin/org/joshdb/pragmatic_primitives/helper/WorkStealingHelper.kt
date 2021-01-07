package org.joshdb.pragmatic_primitives.helper

import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min
import org.joshdb.pragmatic_primitives.DataRecord

@ExperimentalStdlibApi
object WorkStealingHelper : BaseHelper() {

  sealed class NextIndexReturnValue(val nextIndex: Int) {
    class ContinueInBucket(nextIndex: Int) : NextIndexReturnValue(nextIndex)
    class WrapToNextThread(nextIndex: Int, val newEffectiveThreadNumber: Int) :
        NextIndexReturnValue(nextIndex)
  }

  /**
   * for the first iteration you should specify -myHelperNumber (where myHelperNumber is 1-based) as
   * prevIndex
   */
  fun nextIndex(
      prevIndex: Int, effectiveNumThreadsHelping: Int, effectiveHelperNumber: Int, numRecords: Int
  ): NextIndexReturnValue {
    // ok, so there are basically two sensible ways I can think to do this, I propose to eventually
    // make these
    // into two subclasses of this workstealing class and benchmark them to decide which makes more
    // sense,
    // but will just describe them here for now and go with the way that seems cooler for now. I
    // think for all of
    // them it makes sense to have threads that are greater than the closest power of two less than
    // numThreadsHelping
    // just duplicate work so there will some bit twiddling to figure out the appropriate effective
    // thread number
    // no matter what.

    // First is a modulo based approach, so basically you take
    // prevIndex + (1 << (mostSignificantBit(threadsHelping) - 1)) and that's your next
    // index (with some additional jiggery pokers needed to handle changes in numThreadsHelping and
    // effectiveHelperNumber)
    // Second is a chunk based approach, in which we treat the list of records as a set of blocks
    // that we split in
    // half for every doubling of the number of effective threads. So we start with one block and
    // one thread
    // then two blocks for two threads, then 4 for 4, etc. This has the benefit of maximizing
    // locality because
    // each thread is just reading sequentially through a list from some starting point, so we make
    // efficient
    // use of the cache. This strategy also has the benefit of repeating less work, since elements
    // at the end
    // are the ones least likely to have been handled by the existing thread.
    // I think the second approach is cooler, so let's go with that for now and see how it works
    // out.

    fun startingPointForEffectiveHelperNumber(effectiveHelperNumber: Int): Int {
      return (numRecords / effectiveNumThreadsHelping) * effectiveHelperNumber
    }
    val myBucketStartingPoint = startingPointForEffectiveHelperNumber(effectiveHelperNumber)

    val myBucketEndPoint =
        min((numRecords / effectiveNumThreadsHelping) * (effectiveHelperNumber + 1), numRecords)

    // hate to do branching in such a hot path, seems like there's probably some bit-twiddly magic
    // we could
    // avail ourselves of here, but doing ugly if thens for now to express intent
    if (prevIndex < myBucketStartingPoint) {
      // definitely hit this case on the first go, don't think there are any other cases though
      // since this
      // is the function that gives the signal that we've hit the wraparound condition
      return NextIndexReturnValue.ContinueInBucket(myBucketStartingPoint)
    } else if ((prevIndex + 1) < myBucketEndPoint) {
      return NextIndexReturnValue.ContinueInBucket(prevIndex + 1)
    } else {
      // ideally we would do a fast random mod numBuckets here to avoid most pessimal distribution
      // but going sequentially will work for now
      val newEffectiveHelperNumber = (effectiveHelperNumber + 1) % effectiveNumThreadsHelping
      return NextIndexReturnValue.WrapToNextThread(
          startingPointForEffectiveHelperNumber(newEffectiveHelperNumber), newEffectiveHelperNumber)
    }
  }

  override suspend fun help(scxRecord: DataRecord.ScxRecord): Boolean {
    // todo this is the place I think I
    //  have a fun twist on the cooperative multithreading approach
    //  described in the paper that I believe adds a fairly significant progress property

    // super early exit to cut down on unnecessary administration overhead
    if (scxRecord.allFrozen) {
      return true
    }

    // after completing our work we don't have a great way to tell that we're finished doing other
    // thread's work
    // so we just keep track of when we've our work and when we've done ALL the threads' work
    var numThreadSwitchesDone = 0

    // we want to get this first to make sure that we don't complete unneccesary work later on
    var numThreadsCompleted = scxRecord.threadsCompleted.get()
    // it is helpful to note that threadsHelping is effectively monotonically increasing so long as
    // the
    // cooperative part of the scx is still ongoing. Any threads that crash will of course stop
    // contributing
    // but our wraparound strategy to deal with NUMA also deals with this case.
    val myHelperNumber = scxRecord.threadsHelping.getAndIncrement()
    var numThreadsHelping = myHelperNumber + 1

    var effectiveNumThreadsHelping = Integer.highestOneBit(numThreadsHelping)

    var effectiveHelperNumber = myHelperNumber % effectiveNumThreadsHelping

    try {

      var idx =
          nextIndex(
              prevIndex = -1,
              effectiveNumThreadsHelping = effectiveNumThreadsHelping,
              effectiveHelperNumber = effectiveHelperNumber,
              numRecords = scxRecord.affectedRecords.size)

      while (true) {
        // not the most descriptive variable names, copying from Trevor Brown's pseudocode
        // for a different type of clarity
        val rinfo = scxRecord.infoFields.getOrNull(idx.nextIndex)
        val r = scxRecord.affectedRecords[idx.nextIndex]

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

        // if we were done according to the count we were using to calculate the index we just
        // finished then
        // we're finished even if new threads have joined since then
        val oldNumThreadsHelping = numThreadsHelping

        numThreadsHelping = scxRecord.threadsHelping.get()
        effectiveNumThreadsHelping = Integer.highestOneBit(numThreadsHelping)
        effectiveHelperNumber = myHelperNumber % effectiveNumThreadsHelping

        idx =
            nextIndex(
                prevIndex = idx.nextIndex,
                effectiveNumThreadsHelping = effectiveNumThreadsHelping,
                effectiveHelperNumber = effectiveHelperNumber,
                numRecords = scxRecord.affectedRecords.size)

        if (idx is NextIndexReturnValue.WrapToNextThread) {
          if (numThreadSwitchesDone == 0) {
            val newCompletedThreads = scxRecord.threadsCompleted.incrementAndGet()
            if (numThreadsCompleted + 1 >= oldNumThreadsHelping) {
              // all done, regardless of new threads joining
              break
            }

            // if not done we just update the new state of the world
            numThreadsCompleted = newCompletedThreads
            numThreadSwitchesDone += 1
          } else if (numThreadSwitchesDone >= oldNumThreadsHelping) {
            // analogous logic for being done here
            break
          }
          // todo this is not great, loop is not currently structured in a way that supports this
          // logic
          // need to fix that
          // effectiveHelperNumber = idx.newEffectiveThreadNumber
        }
      }

      scxRecord.allFrozen = true

      scxRecord.recordsToFinalize.forEach { record -> record.marked = true }

      scxRecord.recordToModify
          ?.mutableFields
          ?.compareAndExchange(scxRecord.fieldToModify, scxRecord.oldValue, scxRecord.newValue)

      scxRecord.state = DataRecord.ScxRecord.ScxState.COMMITTED
      return true
    } finally {
      scxRecord.threadsHelping.decrementAndGet()
    }
  }
}
