
object FriendshipHelpers {
  val FAMILY = Integer.parseInt(                  "1", 2)
  val CLOSE_FAMILY = Integer.parseInt(            "10", 2)
  val CLOSE_FRIENDS = Integer.parseInt(           "100", 2)
  val SCHOOL = Integer.parseInt(                  "1000", 2)
  val WORK = Integer.parseInt(                    "10000", 2)
  val UNIVERSITY = Integer.parseInt(              "100000", 2)
  val ARMY = Integer.parseInt(                    "1000000", 2)
  val PLAY = Integer.parseInt(                    "10000000", 2)

  val FAMILY_COUNT_MASK = Integer.parseInt(       "111", 2)
  val FAMILY_COUNT_OFFSET =                       0
  val CLOSE_FAMILY_COUNT_MASK = Integer.parseInt( "111000", 2)
  val CLOSE_FAMILY_COUNT_OFFSET =                 3
  val CLOSE_FRIENDS_COUNT_MASK = Integer.parseInt("111000000", 2)
  val CLOSE_FRIENDS_COUNT_OFFSET =                6
  val SCHOOL_COUNT_MASK = Integer.parseInt(       "111000000000", 2)
  val SCHOOL_COUNT_OFFSET =                       9
  val WORK_COUNT_MASK = Integer.parseInt(         "111000000000000", 2)
  val WORK_COUNT_OFFSET =                         12
  val UNIVERSITY_COUNT_MASK = Integer.parseInt(   "111000000000000000", 2)
  val UNIVERSITY_COUNT_OFFSET =                   15
  val ARMY_COUNT_MASK = Integer.parseInt(         "111000000000000000000", 2)
  val ARMY_COUNT_OFFSET =                         18
  val PLAY_COUNT_MASK = Integer.parseInt(         "111000000000000000000000", 2)
  val PLAY_COUNT_OFFSET =                         21

  val M_LOVE = Integer.parseInt(                  "10", 2)
  val M_SPOUSE = Integer.parseInt(                "100", 2)
  val M_PARENT = Integer.parseInt(                "1000", 2)
  val M_CHILD = Integer.parseInt(                 "10000", 2)
  val M_BROTHER_SISTER = Integer.parseInt(        "100000", 2)
  val M_UNCLE_AUNT = Integer.parseInt(            "1000000", 2)
  val M_RELATIVE = Integer.parseInt(              "10000000", 2)
  val M_CLOSE_FRIEND = Integer.parseInt(          "100000000", 2)
  val M_COLLEAGUE = Integer.parseInt(             "1000000000", 2)
  val M_SCHOOLMATE = Integer.parseInt(            "10000000000", 2)
  val M_NEPHEW = Integer.parseInt(                "100000000000", 2)
  val M_GRANDPARENT = Integer.parseInt(           "1000000000000", 2)
  val M_GRANDCHILD = Integer.parseInt(            "10000000000000", 2)
  val M_UNIVERSITY_FEL = Integer.parseInt(        "100000000000000", 2)
  val M_ARMY_FEL = Integer.parseInt(              "1000000000000000", 2)
  val M_PARENT_IN_LAW = Integer.parseInt(         "10000000000000000", 2)
  val M_CHILD_IN_LAW = Integer.parseInt(          "100000000000000000", 2)
  val M_GODPARENT = Integer.parseInt(             "1000000000000000000", 2)
  val M_GODCHILD = Integer.parseInt(              "10000000000000000000", 2)
  val M_PLAY_TOGETHER = Integer.parseInt(         "100000000000000000000", 2)

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

  protected def granulateFriendshipType(fType: Int): Int = {
    var result = 0

    if (fType == 0) {
      return result
    }

    if ((fType & M_LOVE) > 0) {
      result |= CLOSE_FRIENDS
      result |= SCHOOL
    }

    if ((fType & M_SPOUSE) > 0) {
      result |= CLOSE_FAMILY
      result |= FAMILY
      result |= CLOSE_FRIENDS
    }

    if ((fType & M_PARENT) > 0) {
      result |= CLOSE_FAMILY
      result |= FAMILY
      result |= CLOSE_FRIENDS
    }

    if ((fType & M_CHILD) > 0) {
      result |= CLOSE_FAMILY
      result |= FAMILY
    }

    if ((fType & M_BROTHER_SISTER) > 0) {
      result |= CLOSE_FAMILY
      result |= FAMILY
    }

    if ((fType & M_UNCLE_AUNT) > 0) {
      result |= FAMILY
    }

    if ((fType & M_RELATIVE) > 0) {
      result |= FAMILY
    }

    if ((fType & M_CLOSE_FRIEND) > 0) {
      result |= CLOSE_FAMILY
      result |= CLOSE_FRIENDS
    }

    if ((fType & M_COLLEAGUE) > 0) {
      result |= WORK
    }

    if ((fType & M_SCHOOLMATE) > 0) {
      result |= SCHOOL
    }

    if ((fType & M_NEPHEW) > 0) {
      result |= FAMILY
    }

    if ((fType & M_GRANDPARENT) > 0) {
      result |= FAMILY
    }

    if ((fType & M_GRANDCHILD) > 0) {
      result |= FAMILY
    }

    if ((fType & M_UNIVERSITY_FEL) > 0) {
      result |= UNIVERSITY
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

  protected def mainGranulateFriendshipType(fType: Int): Int = {
    var result = 0

    if (fType == 0) {
      return result
    }

    if ((fType & M_LOVE) > 0) {
      result |= CLOSE_FRIENDS
    }

    if ((fType & M_SPOUSE) > 0) {
      result |= CLOSE_FAMILY
    }

    if ((fType & M_PARENT) > 0) {
      result |= CLOSE_FAMILY
    }

    if ((fType & M_CHILD) > 0) {
      result |= CLOSE_FAMILY
    }

    if ((fType & M_BROTHER_SISTER) > 0) {
      result |= CLOSE_FAMILY
    }

    if ((fType & M_UNCLE_AUNT) > 0) {
      result |= FAMILY
    }

    if ((fType & M_RELATIVE) > 0) {
      result |= FAMILY
    }

    if ((fType & M_CLOSE_FRIEND) > 0) {
      result |= CLOSE_FRIENDS
    }

    if ((fType & M_COLLEAGUE) > 0) {
      result |= WORK
    }

    if ((fType & M_SCHOOLMATE) > 0) {
      result |= SCHOOL
    }

    if ((fType & M_NEPHEW) > 0) {
      result |= FAMILY
    }

    if ((fType & M_GRANDPARENT) > 0) {
      result |= FAMILY
    }

    if ((fType & M_GRANDCHILD) > 0) {
      result |= FAMILY
    }

    if ((fType & M_UNIVERSITY_FEL) > 0) {
      result |= UNIVERSITY
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

  protected def getMainFriendshipFromMask(granulatedFType: Int): Int = {
    if ((granulatedFType & CLOSE_FAMILY) > 0) {
      return CLOSE_FAMILY
    }

    if ((granulatedFType & CLOSE_FRIENDS) > 0) {
      return CLOSE_FRIENDS
    }

    if ((granulatedFType & FAMILY) > 0) {
      return FAMILY
    }

    if ((granulatedFType & SCHOOL) > 0) {
      return SCHOOL
    }

    if ((granulatedFType & WORK) > 0) {
      return WORK
    }

    if ((granulatedFType & UNIVERSITY) > 0) {
      return UNIVERSITY
    }

    if ((granulatedFType & ARMY) > 0) {
      return ARMY
    }

    if ((granulatedFType & PLAY) > 0) {
      return PLAY
    }

    0
  }

  def combineFriendshipTypesToGranulatedFType(fType1: Int, fType2: Int): Int = {
    if (fType1 == 0 || fType2 == 0) {
      0
    } else if (fType1 == fType2) {
      getMainFriendshipFromMask(mainGranulateFriendshipType(fType1))
    } else {
      getMainFriendshipFromMask(granulateFriendshipType(fType1) & granulateFriendshipType(fType2))
    }
  }

  def granulatedFTypeToFAccumulator(granulatedFType: Int): Int = {
    if (granulatedFType == 0) {
      return 0
    }
    var result = 0
    result = accumulateOneGranulatedFType(result, granulatedFType, FAMILY, FAMILY_COUNT_MASK, FAMILY_COUNT_OFFSET)
    result = accumulateOneGranulatedFType(result, granulatedFType, CLOSE_FRIENDS, CLOSE_FRIENDS_COUNT_MASK, CLOSE_FRIENDS_COUNT_OFFSET)
    result = accumulateOneGranulatedFType(result, granulatedFType, SCHOOL, SCHOOL_COUNT_MASK, SCHOOL_COUNT_OFFSET)
    result = accumulateOneGranulatedFType(result, granulatedFType, WORK, WORK_COUNT_MASK, WORK_COUNT_OFFSET)
    result = accumulateOneGranulatedFType(result, granulatedFType, UNIVERSITY, UNIVERSITY_COUNT_MASK, UNIVERSITY_COUNT_OFFSET)
    result = accumulateOneGranulatedFType(result, granulatedFType, ARMY, ARMY_COUNT_MASK, ARMY_COUNT_OFFSET)
    result = accumulateOneGranulatedFType(result, granulatedFType, PLAY, PLAY_COUNT_MASK, PLAY_COUNT_OFFSET)
    result
  }

  def mergeAccumulators(accumulator1: Int, accumulator2: Int): Int = {
    if (accumulator1 == 0) {
      return accumulator2
    }
    if (accumulator2 == 0) {
      return accumulator1
    }

    var result = accumulator1
    result = mergeAccumulators(result, accumulator2, FAMILY_COUNT_MASK, FAMILY_COUNT_OFFSET)
    result = mergeAccumulators(result, accumulator2, CLOSE_FRIENDS_COUNT_MASK, CLOSE_FRIENDS_COUNT_OFFSET)
    result = mergeAccumulators(result, accumulator2, SCHOOL_COUNT_MASK, SCHOOL_COUNT_OFFSET)
    result = mergeAccumulators(result, accumulator2, WORK_COUNT_MASK, WORK_COUNT_OFFSET)
    result = mergeAccumulators(result, accumulator2, UNIVERSITY_COUNT_MASK, UNIVERSITY_COUNT_OFFSET)
    result = mergeAccumulators(result, accumulator2, ARMY_COUNT_MASK, ARMY_COUNT_OFFSET)
    result = mergeAccumulators(result, accumulator2, PLAY_COUNT_MASK, PLAY_COUNT_OFFSET)
    result
  }

  private def accumulateOneGranulatedFType(accumulator: Int, granulatedFType: Int, granulationType: Int, granulationMask: Int, granulationOffset: Int): Int = {
    if ((granulatedFType & granulationType) > 0) {
      val currentCount = (accumulator & granulationMask) >> granulationOffset
      val count = currentCount + 1
      if (count < 8) {
        (accumulator & ~granulationMask) + (count << granulationOffset)
      } else {
        accumulator
      }
    } else {
      accumulator
    }
  }

  private def mergeAccumulators(accumulator1: Int, accumulator2: Int, granulationMask: Int, granulationOffset: Int): Int = {
    val count = getGranulatedFTypeCountFromAccumulator(accumulator1, granulationMask,  granulationOffset) +
        getGranulatedFTypeCountFromAccumulator(accumulator2, granulationMask, granulationOffset)
    if (count < 8) {
      (accumulator1 & ~granulationMask) + (count << granulationOffset)
    } else {
      (accumulator1 & ~granulationMask) + (7 << granulationOffset)
    }
  }

  def getGranulatedFTypeCountFromAccumulator(accumulator: Int, granulationMask: Int, granulationOffset: Int): Int = {
    (accumulator & granulationMask) >> granulationOffset
  }

  def getCoefForCombinedFriendship(combinedFType: Int): Double = {
    var result = 1.0

    if ((combinedFType & FAMILY) > 0) {
      result += 1.0
    }

    if ((combinedFType & CLOSE_FRIENDS) > 0) {
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


