package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.lore.OutputLoreId
import org.scalatest.funsuite.AnyFunSuite

class OutputLoreIdSuite extends AnyFunSuite {
  test("Parse one output lore id") {
    val expectedLoreIds = Map(1 -> OutputLoreId(1, Set(1, 2, 4, 8)))
    val loreIds = OutputLoreId.parse("1[1 2 4 8]")

    assert(loreIds == expectedLoreIds)
  }

  test("Parse multi output lore id") {
    val expectedLoreIds = Map(
      1 -> OutputLoreId(1, Set(1, 2, 4, 8)),
      2 -> OutputLoreId(2, Set(1, 4, 5, 6, 7, 8, 100))
    )
    val loreIds = OutputLoreId.parse("1[1 2 4 8], 2[1 4-9 100]")

    assert(loreIds == expectedLoreIds)
  }

  test("Parse empty output lore id") {
    val expectedLoreIds = Map(1 -> OutputLoreId(1), 2 -> OutputLoreId(2))
    val loreIds = OutputLoreId.parse("1 , 2")

    assert(loreIds == expectedLoreIds)
  }

  test("Parse mixed") {
    val expectedLoreIds = Map(
      1 -> OutputLoreId(1),
      2 -> OutputLoreId(2, Set(4, 5, 8)),
      3 -> OutputLoreId(3, Set(1, 2, 4, 8))
    )
    val loreIds = OutputLoreId.parse("1, 2[4-6 8] , 3[1 2 4 8]")

    assert(loreIds == expectedLoreIds)
  }
}
