//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer_test.cpp
//
// Identification: test/buffer/lru_replacer_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <thread>  // NOLINT
#include <vector>

#include "buffer/lru_replacer.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(LRUReplacerTest, SampleTest) {
  LRUReplacer lru_replacer(7);

  // Scenario: unpin six elements, i.e. add them to the replacer.
  lru_replacer.Unpin(1);
  lru_replacer.Unpin(2);
  lru_replacer.Unpin(3);
  lru_replacer.Unpin(4);
  lru_replacer.Unpin(5);
  lru_replacer.Unpin(8);
  lru_replacer.Unpin(1);
  EXPECT_EQ(6, lru_replacer.Size());

  // Scenario: get three victims from the lru.
  int value;
  lru_replacer.Victim(&value);
  EXPECT_EQ(1, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(2, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(3, value);

  // Scenario: pin elements in the replacer.
  // Note that 3 has already been victimized, so pinning 3 should have no effect.
  lru_replacer.Pin(3);
  lru_replacer.Pin(4);
  EXPECT_EQ(2, lru_replacer.Size());

  // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
  lru_replacer.Unpin(4);

  // Scenario: continue looking for victims. We expect these victims.
  lru_replacer.Victim(&value);
  EXPECT_EQ(5, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(8, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(4, value);
}
/*
TEST(LRUReplacerTest, cloudbreak) {
  LRUReplacer lru_replacer(2);
  // initialize LRU
  lru_replacer.Unpin(0);
  lru_replacer.Unpin(1);

  // frame 0 is occupied by long running query.
  lru_replacer.Pin(0);

  // frame 1 is used by multiple small requests and is hot frame
  lru_replacer.Pin(1);
  lru_replacer.Pin(1);
  lru_replacer.Pin(1);

  // all small queries finish
  // Unpin is called only once after all requests finish.
  lru_replacer.Unpin(1);
  // long query finishes
  lru_replacer.Unpin(0);

  // Now, which page should get victim/swapped out to disk ?
  // I expect 0 as that is not used by many requests lately.
  int temp;
  EXPECT_TRUE(lru_replacer.Victim(&temp));
  EXPECT_EQ(0, temp);
}
*/
}  // namespace bustub
