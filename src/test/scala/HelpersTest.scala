import org.scalatest._

class HelpersTest extends FlatSpec {
  "normalizeFriendshipType" should "remove 0 bit" in {
    assert(Helpers.normalizeFriendshipType(16) == Helpers.normalizeFriendshipType(17))
    assert(Helpers.normalizeFriendshipType(16) == 16)
  }

  "inversion for Nephew" should "be Aunt" in {
    assert(Helpers.invertFriendshipType(1 << 11) == (1 << 6))
    assert(Helpers.invertFriendshipType((1 << 11) + 1) == (1 << 6) + 1)
    assert(Helpers.invertFriendshipType((1 << 11) + 2) == (1 << 6) + 2)
  }

  "Spouse And Parent" should "have same bit in combination" in {
    assert(Helpers.combineFriendshipTypesToMask(1 << 2, 1 << 3) > 0)
  }

  "Spouse And Army" should "have nothing similar in combination" in {
    assert(Helpers.combineFriendshipTypesToMask(1 << 2, 1 << 15) == 0)
  }
}


