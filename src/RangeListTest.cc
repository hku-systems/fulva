#include "TestUtil.h"
#include "RangeList.h"

namespace RAMCloud {

struct RangeListTest : public ::testing::Test {

    RangeListTest()
    {

    }

  private:
    DISALLOW_COPY_AND_ASSIGN(RangeListTest);
};

TEST_F(RangeListTest, initialization)
{
    RangeList rangeList(0, 100);
    rangeList.lock(0);
    rangeList.lock(1);
    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 6; j++) {
            rangeList.lock(static_cast<uint64_t>(i * 20 + j));
        }
    }

    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 6; j++) {
            rangeList.unlock(static_cast<uint64_t>(i * 20 + j));
        }
    }
}

TEST_F(RangeListTest, construction)
{
    RangeList rangeList;
    WireFormat::MigrationIsLocked::Range range;
    range.start = 1;
    range.end = 9;
    rangeList.push(range);

    EXPECT_FALSE(rangeList.isLocked(5));

    for (uint64_t i = 1; i < 4; i++) {
        range.start = i * 10;
        range.end = i * 10 + 6;
        rangeList.push(range);
    }

    EXPECT_TRUE(rangeList.isLocked(27));
    for (uint64_t i = 1; i < 4; i++) {
        range.start = i * 10 + 7;
        range.end = i * 10 + 9;
        rangeList.push(range);
    }
    EXPECT_FALSE(rangeList.isLocked(27));

}


TEST_F(RangeListTest, lockAndRebuild)
{

    RangeList rangeList(0, 100);
    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 6; j++) {
            rangeList.lock(static_cast<uint64_t>(i * 20 + j));
        }
    }

    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 5; j++) {
            rangeList.unlock(static_cast<uint64_t>(i * 20 + j));
        }
    }

    vector<WireFormat::MigrationIsLocked::Range> ranges;

    RangeList rebuild;
    rangeList.getRanges(ranges);
    for (auto range : ranges) {
        rebuild.push(range);
    }

    EXPECT_TRUE(rebuild.isLocked(5));
    for (int i = 0; i < 4; i++) {
        rangeList.unlock(static_cast<uint64_t>(i * 20 + 5));
    }

    ranges.clear();
    rangeList.getRanges(ranges);
    for (auto range : ranges) {
        rebuild.push(range);
    }
    EXPECT_FALSE(rebuild.isLocked(5));
}

TEST_F(RangeListTest, largeRebuild)
{
    RangeList rangeList(0, 10000);
    for (int i = 0; i < 400; i++) {
        for (int j = 0; j < 6; j++) {
            rangeList.lock(static_cast<uint64_t>(i * 20 + j));
        }
    }

    for (int i = 0; i < 400; i++) {
        for (int j = 0; j < 5; j++) {
            rangeList.unlock(static_cast<uint64_t>(i * 20 + j));
        }
    }

    vector<WireFormat::MigrationIsLocked::Range> ranges;

    RangeList rebuild;
    rangeList.getRanges(ranges);
    for (auto range : ranges) {
        rebuild.push(range);
    }

    EXPECT_TRUE(rebuild.isLocked(5));
    for (int i = 0; i < 400; i++) {
        rangeList.unlock(static_cast<uint64_t>(i * 20 + 5));
    }

    ranges.clear();
    rangeList.getRanges(ranges);
    for (auto range : ranges) {
        rebuild.push(range);
    }
    EXPECT_FALSE(rebuild.isLocked(5));

    EXPECT_THROW(rangeList.lock(20), FatalError);
}

}

