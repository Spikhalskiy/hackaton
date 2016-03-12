import org.scalatest._

class FriendshipHelpersTest extends FlatSpec {
  "normalizeFriendshipType" should "remove 0 bit" in {
    assert(FriendshipHelpers.normalizeFriendshipType(16) == FriendshipHelpers.normalizeFriendshipType(17))
    assert(FriendshipHelpers.normalizeFriendshipType(16) == 16)
  }

  "inversion for Nephew" should "be Aunt" in {
    assert(FriendshipHelpers.invertFriendshipType(1 << 11) == (1 << 6))
    assert(FriendshipHelpers.invertFriendshipType((1 << 11) + 1) == (1 << 6) + 1)
    assert(FriendshipHelpers.invertFriendshipType((1 << 11) + 2) == (1 << 6) + 2)
  }

  "Spouse And Parent" should "have same bit in combination" in {
    assert(FriendshipHelpers.combineFriendshipTypesToGranulatedFType(1 << 2, 1 << 3) > 0)
  }

  "Spouse And Army" should "have nothing similar in combination" in {
    assert(FriendshipHelpers.combineFriendshipTypesToGranulatedFType(1 << 2, 1 << 15) == 0)
  }

  "Two From Army" should "have coefficient more than 1.0" in {
    assert(FriendshipHelpers.getCoefForCombinedFriendship(FriendshipHelpers.combineFriendshipTypesToGranulatedFType(1 << 6, 1 << 6)) > 1.0)
  }

  "Combined Ftype converted to accumulator" should "produce accumulator with 1 in related group position" in {
    val accumulator = FriendshipHelpers.granulatedFTypeToFAccumulator(
      FriendshipHelpers.combineFriendshipTypesToGranulatedFType(FriendshipHelpers.M_PARENT, FriendshipHelpers.M_GODPARENT))
    assert(FriendshipHelpers.getGranulatedFTypeCountFromAccumulator(accumulator, FriendshipHelpers.FAMILY_COUNT_MASK, FriendshipHelpers.FAMILY_COUNT_OFFSET) == 1)
    assert(FriendshipHelpers.getGranulatedFTypeCountFromAccumulator(accumulator, FriendshipHelpers.ARMY_COUNT_MASK, FriendshipHelpers.ARMY_COUNT_OFFSET) == 0)
  }

  "Combined accumulators" should "produce accumulator with right counts in related group position" in {
    val accumulator1 = FriendshipHelpers.granulatedFTypeToFAccumulator(
      FriendshipHelpers.combineFriendshipTypesToGranulatedFType(FriendshipHelpers.M_PARENT, FriendshipHelpers.M_GODPARENT))
    val accumulator2 = FriendshipHelpers.granulatedFTypeToFAccumulator(
      FriendshipHelpers.combineFriendshipTypesToGranulatedFType(FriendshipHelpers.M_LOVE, FriendshipHelpers.M_CLOSE_FRIEND))
    val accumulator3 = FriendshipHelpers.granulatedFTypeToFAccumulator(
      FriendshipHelpers.combineFriendshipTypesToGranulatedFType(FriendshipHelpers.M_COLLEAGUE, FriendshipHelpers.M_COLLEAGUE))
    val accumulator4 = FriendshipHelpers.granulatedFTypeToFAccumulator(
      FriendshipHelpers.combineFriendshipTypesToGranulatedFType(FriendshipHelpers.M_CLOSE_FRIEND, FriendshipHelpers.M_CLOSE_FRIEND))
    val accumulator = FriendshipHelpers.mergeAccumulators(FriendshipHelpers.mergeAccumulators(FriendshipHelpers.mergeAccumulators(accumulator1, accumulator2), accumulator3), accumulator4)

    assert(FriendshipHelpers.getGranulatedFTypeCountFromAccumulator(accumulator, FriendshipHelpers.FAMILY_COUNT_MASK, FriendshipHelpers.FAMILY_COUNT_OFFSET) == 1)
    assert(FriendshipHelpers.getGranulatedFTypeCountFromAccumulator(accumulator, FriendshipHelpers.CLOSE_FRIENDS_COUNT_MASK, FriendshipHelpers.CLOSE_FRIENDS_COUNT_OFFSET) == 2)
    assert(FriendshipHelpers.getGranulatedFTypeCountFromAccumulator(accumulator, FriendshipHelpers.WORK_COUNT_MASK, FriendshipHelpers.WORK_COUNT_OFFSET) == 1)
  }
}


