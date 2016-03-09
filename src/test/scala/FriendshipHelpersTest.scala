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
    assert(FriendshipHelpers.combineFriendshipTypesToMask(1 << 2, 1 << 3) > 0)
  }

  "Spouse And Army" should "have nothing similar in combination" in {
    assert(FriendshipHelpers.combineFriendshipTypesToMask(1 << 2, 1 << 15) == 0)
  }
}


