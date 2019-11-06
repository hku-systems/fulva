#ifndef RAMCLOUD_RANGETREE_H
#define RAMCLOUD_RANGETREE_H

#include "Common.h"

namespace RAMCloud {
class RangeNode {
  PUBLIC:
    uint64_t start, end;

    RangeNode(uint64_t start, uint64_t end);

    virtual ~RangeNode()
    {}

    virtual bool operator<(uint64_t hash);

    virtual bool operator>(uint64_t hash);

};

class RangeList {
  PUBLIC:

    class LockNode : public RangeNode {
      PUBLIC:
        uint64_t lockNumber;

        std::vector<LockNode *> forward, backward;
        int level;
        bool max, min;
        LockNode *unlockForward, *unlockBackward;

        LockNode(uint64_t start, uint64_t end, int level, bool max = false,
                 bool min = false);

        bool operator<(uint64_t hash);

        bool operator>(uint64_t hash);

        std::string toString();

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(LockNode)
    };

  PRIVATE:

    static const int MAX_LEVEL = 16;

    bool startQuery;
    SpinLock spinLock;

    LockNode *head, *unlockHead, *postHead;
    LockNode *BEGIN, *END;

    int randomLevel();

    LockNode *find(uint64_t hash, vector<LockNode *> &update);

    void insert(std::vector<LockNode *> &update, LockNode *node);

    void remove(LockNode *node);

    void unlockInsert(LockNode *node);

    void unlockRemove(LockNode *node);

  PUBLIC:

    RangeList();

    RangeList(uint64_t start, uint64_t end);

    ~RangeList();

    void lock(uint64_t hash);

    void unlock(uint64_t hash);

    LockNode *getRanges(vector<WireFormat::MigrationIsLocked::Range> &ranges,
                        int num = 1000);

    void push(WireFormat::MigrationIsLocked::Range &range);

    bool isLocked(uint64_t hash);

    void print();

    void printUnlock();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(RangeList)
};

class AvailableRangeList {
  PUBLIC:

    AvailableRangeList();

    void isLocked(uint64_t hash);

    void insert(uint64_t start, uint64_t end);
};

}

#endif //RAMCLOUD_RANGELIST_H
