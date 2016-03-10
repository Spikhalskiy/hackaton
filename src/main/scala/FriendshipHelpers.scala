
object FriendshipHelpers {
  val FAMILY = Integer.parseInt(    "10", 2)
  val FRIENDS = Integer.parseInt(   "100", 2)
  val SCHOOL = Integer.parseInt(    "1000", 2)
  val WORK = Integer.parseInt(      "10000", 2)
  val UNIVERSITY = Integer.parseInt("100000", 2)
  val ARMY = Integer.parseInt(      "1000000", 2)
  val PLAY = Integer.parseInt(      "10000000", 2)

  val M_LOVE = Integer.parseInt(            "10", 2)
  val M_SPOUSE = Integer.parseInt(          "100", 2)
  val M_PARENT = Integer.parseInt(          "1000", 2)
  val M_CHILD = Integer.parseInt(           "10000", 2)
  val M_BROTHER_SISTER = Integer.parseInt(  "100000", 2)
  val M_UNCLE_AUNT = Integer.parseInt(      "1000000", 2)
  val M_RELATIVE = Integer.parseInt(        "10000000", 2)
  val M_CLOSE_FRIEND = Integer.parseInt(    "100000000", 2)
  val M_COLLEAGUE = Integer.parseInt(       "1000000000", 2)
  val M_SCHOOLMATE = Integer.parseInt(      "10000000000", 2)
  val M_NEPHEW = Integer.parseInt(          "100000000000", 2)
  val M_GRANDPARENT = Integer.parseInt(     "1000000000000", 2)
  val M_GRANDCHILD = Integer.parseInt(      "10000000000000", 2)
  val M_UNIVERSITY_FEL = Integer.parseInt(  "100000000000000", 2)
  val M_ARMY_FEL = Integer.parseInt(        "1000000000000000", 2)
  val M_PARENT_IN_LAW = Integer.parseInt(   "10000000000000000", 2)
  val M_CHILD_IN_LAW = Integer.parseInt(    "100000000000000000", 2)
  val M_GODPARENT = Integer.parseInt(       "1000000000000000000", 2)
  val M_GODCHILD = Integer.parseInt(        "10000000000000000000", 2)
  val M_PLAY_TOGETHER = Integer.parseInt(   "100000000000000000000", 2)

  def normalizeFriendshipType(fType: Int): Int = {
    fType - fType % 2
  }

  def invertFriendshipType(fType: Int): Int = {
    var result = fType
    if ((result & M_PARENT) > 0) {
      result -= M_PARENT
      result |= M_CHILD
    }

    if ((result & M_CHILD) > 0) {
      result -= M_CHILD
      result |= M_PARENT
    }

    if ((result & M_UNCLE_AUNT) > 0) {
      result -= M_UNCLE_AUNT
      result |= M_NEPHEW
    }

    if ((result & M_NEPHEW) > 0) {
      result -= M_NEPHEW
      result |= M_UNCLE_AUNT
    }

    if ((result & M_GRANDPARENT) > 0) {
      result -= M_GRANDPARENT
      result |= M_GRANDCHILD
    }

    if ((result & M_PARENT_IN_LAW) > 0) {
      result -= M_PARENT_IN_LAW
      result |= M_CHILD_IN_LAW
    }

    if ((result & M_CHILD_IN_LAW) > 0) {
      result -= M_CHILD_IN_LAW
      result |= M_PARENT_IN_LAW
    }

    if ((result & M_GODPARENT) > 0) {
      result -= M_GODPARENT
      result |= M_GODCHILD
    }

    if ((result & M_GODCHILD) > 0) {
      result -= M_GODCHILD
      result |= M_GODPARENT
    }

    result
  }

  def granulateFriendshipType(fType: Int): Int = {
    var result = 0

    //1 - family
    //2 - friends
    //3 - school
    //4 - work
    //5 - university
    //6 - army
    //7 - play

    if ((fType & M_LOVE) > 0) {
      result |= FAMILY
      result |= FRIENDS
      result |= UNIVERSITY
      result |= SCHOOL
      result |= PLAY
    }

    if ((fType & M_SPOUSE) > 0) {
      result |= FAMILY
      result |= FRIENDS
    }

    if ((fType & M_PARENT) > 0) {
      result |= FAMILY
      result |= SCHOOL
    }

    if ((fType & M_CHILD) > 0) {
      result |= FAMILY
    }

    if ((fType & M_BROTHER_SISTER) > 0) {
      result |= FAMILY
    }

    if ((fType & M_UNCLE_AUNT) > 0) {
      result |= FAMILY
    }

    if ((fType & M_RELATIVE) > 0) {
      result |= FAMILY
    }

    if ((fType & M_CLOSE_FRIEND) > 0) {
      result |= FAMILY
      result |= FRIENDS
    }

    if ((fType & M_COLLEAGUE) > 0) {
      result |= WORK
    }

    if ((fType & M_SCHOOLMATE) > 0) {
      result |= FRIENDS
      result |= SCHOOL
    }

    if ((fType & M_NEPHEW) > 0) {
      result |= FAMILY
    }

    if ((fType & M_GRANDPARENT) > 0) {
      result |= FAMILY
      result |= SCHOOL
    }

    if ((fType & M_GRANDCHILD) > 0) {
      result |= FAMILY
    }

    if ((fType & M_UNIVERSITY_FEL) > 0) {
      result |= UNIVERSITY
      result |= FRIENDS
    }

    if ((fType & M_ARMY_FEL) > 0) {
      result |= ARMY
    }

    if ((fType & M_PARENT_IN_LAW) > 0) {
      result |= FAMILY
    }

    if ((fType & M_CHILD_IN_LAW) > 0) {
      result |= FAMILY
    }

    if ((fType & M_GODPARENT) > 0) {
      result |= FAMILY
    }

    if ((fType & M_GODCHILD) > 0) {
      result |= FAMILY
    }

    if ((fType & M_PLAY_TOGETHER) > 0) {
      result |= PLAY
    }

    result
  }

  def combineFriendshipTypesToMask(fType1: Int, fType2: Int): Int = {
    granulateFriendshipType(fType1) & granulateFriendshipType(fType2)
  }

  def getCoefForCombinedFriendship(combinedFType: Int): Double = {
    var result = 1.0

    if ((combinedFType & FAMILY) > 0) {
      result += 1.0
    }

    if ((combinedFType & FRIENDS) > 0) {
      result += 0.1
    }

    if ((combinedFType & SCHOOL) > 0) {
      result += 0.6
    }

    if ((combinedFType & WORK) > 0) {
      result += 0.3
    }

    if ((combinedFType & UNIVERSITY) > 0) {
      result += 0.5
    }

    if ((combinedFType & ARMY) > 0) {
      result += 0.4
    }

    if ((combinedFType & PLAY) > 0) {
      result += 0.7
    }

    result
  }
}


