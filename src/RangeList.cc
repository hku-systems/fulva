#include "RangeList.h"
#include <iostream>

namespace RAMCloud {

RangeNode::RangeNode(uint64_t start, uint64_t end)
    : start(start), end(end)
{

}

bool RangeNode::operator<(uint64_t hash)
{
    return end < hash;
}

bool RangeNode::operator>(uint64_t hash)
{
    return start > hash;
}

RangeList::LockNode::LockNode(uint64_t start, uint64_t end, int level, bool max,
                              bool min)
    : RangeNode(start, end), lockNumber(0), forward(), backward(), level(level),
      max(max), min(min), unlockForward(NULL), unlockBackward(NULL)
{
    for (int i = 0; i < level; ++i) {
        forward.emplace_back(nullptr);
        backward.emplace_back(nullptr);
    }
}

bool RangeList::LockNode::operator<(uint64_t hash)
{
    return !max && (min || RangeNode::operator<(hash));
}

bool RangeList::LockNode::operator>(uint64_t hash)
{
    return !min && (max || RangeNode::operator>(hash));
}

std::string RangeList::LockNode::toString()
{
    return format("[start=%lu, end=%lu, lock=%lu]", start, end, lockNumber);
}

RangeList::RangeList() :
    startQuery(false), spinLock("RangeList"), head(NULL), unlockHead(NULL),
    postHead(NULL), BEGIN(NULL), END(NULL)
{
    BEGIN = new LockNode(~0ul, ~0ul, MAX_LEVEL, false, true);
    BEGIN->lockNumber = ~0ul;
    END = new LockNode(0, 0, MAX_LEVEL, true, false);
    END->lockNumber = ~0ul;
    for (int i = 0; i < BEGIN->level; i++) {
        BEGIN->forward[i] = END;
        END->backward[i] = BEGIN;
    }
    BEGIN->unlockForward = END;
    head = BEGIN;
    unlockHead = BEGIN;
}

RangeList::RangeList(uint64_t start, uint64_t end) : RangeList()
{
    LockNode *init = new LockNode(start, end, randomLevel());
    for (int i = 0; i < init->level; i++) {
        init->forward[i] = head->forward[i];
        init->backward[i] = head;
        head->forward[i] = init;
    }
    unlockInsert(init);
}

RangeList::~RangeList()
{
    delete BEGIN;
    delete END;
}

int RangeList::randomLevel()
{
    int v = 1;

    while (std::rand() % 2 == 0 && v < MAX_LEVEL) {
        v += 1;
    }
    return v;

}

RangeList::LockNode *
RangeList::find(uint64_t hash, vector<RangeList::LockNode *> &update)
{

    LockNode *node = head;
    for (auto i : head->forward) {
        update.emplace_back(i);
    }

    for (int l = node->level - 1; l >= 0; l--) {
        while (node->forward[l] != NULL && *(node->forward[l]) < hash) {
            node = node->forward[l];
        }
        update[l] = node;
    }

    return node;
}

void RangeList::insert(vector<RangeList::LockNode *> &update,
                       RangeList::LockNode *node)
{
    for (int i = 0; i < node->level; i++) {
        node->forward[i] = update[i]->forward[i];
        node->backward[i] = update[i];
        update[i]->forward[i] = node;
        if (node->forward[i])
            node->forward[i]->backward[i] = node;
    }
}


void RangeList::remove(RangeList::LockNode *node)
{
    for (int i = 0; i < node->level; i++) {
        node->backward[i]->forward[i] = node->forward[i];
        node->forward[i]->backward[i] = node->backward[i];
    }

    delete node;
}

void RangeList::lock(uint64_t hash)
{
    SpinLock::Guard guard(spinLock);

    if (startQuery)
        throw FatalError(HERE, "Still lock while RangeList start query");

    LockNode *node;
    vector<LockNode *> update;

    node = find(hash, update);
    node = node->forward[0];

    if (node->lockNumber == 0) {
        LockNode *preNode = NULL;
        LockNode *postNode = NULL;
        if (node->start < hash)
            preNode = new LockNode(node->start, hash - 1, randomLevel());
        if (hash < node->end)
            postNode = new LockNode(hash + 1, node->end, randomLevel());
        node->start = hash;
        node->end = hash;
        node->lockNumber = 1;

        if (preNode != NULL) {
            insert(update, preNode);
            unlockInsert(preNode);
        }
        if (postNode != NULL) {
            for (int i = postNode->level - 1; i >= 0; i--) {
                if (i < node->level) {
                    update[i] = node;
                }
            }
            insert(update, postNode);
            unlockInsert(postNode);
        }
        unlockRemove(node);
    } else {
        node->lockNumber++;
    }
}

void RangeList::unlock(uint64_t hash)
{
    SpinLock::Guard guard(spinLock);
    if (startQuery) {
        LockNode *node = new LockNode(hash, hash, 1);
        if (postHead) {
            node->unlockForward = postHead;
            postHead = node;
        } else {
            postHead = node;
        }
        return;
    }

    LockNode *node;
    vector<LockNode *> update;
    node = find(hash, update);
    LockNode *preNode = node;

    node = node->forward[0];
    LockNode *postNode = node->forward[0];

    node->lockNumber--;

    if (node->lockNumber == 0) {
        unlockInsert(node);
        if (preNode->lockNumber == 0) {
            uint64_t preStart = preNode->start;
            unlockRemove(preNode);
            remove(preNode);
            node->start = preStart;
        }
        if (postNode->lockNumber == 0) {
            uint64_t postEnd = postNode->end;
            unlockRemove(postNode);
            remove(postNode);
            node->end = postEnd;
        }
    }
}

RangeList::LockNode *
RangeList::getRanges(vector<WireFormat::MigrationIsLocked::Range> &ranges,
                     int num)
{
    {
        SpinLock::Guard guard(spinLock);
        startQuery = true;
    }

    if (unlockHead == BEGIN)
        unlockHead = unlockHead->unlockForward;
    int i = 0;
    while (unlockHead != END) {
        if (i >= num)
            break;
        i++;
        ranges.push_back(WireFormat::MigrationIsLocked::Range
                             {unlockHead->start, unlockHead->end});
        unlockHead = unlockHead->unlockForward;
    }


    {
        SpinLock::Guard guard(spinLock);
        while (postHead != NULL) {
            if (i >= num)
                break;
            i++;
            ranges.push_back(WireFormat::MigrationIsLocked::Range
                                 {postHead->start, postHead->end});
            postHead = postHead->unlockForward;
        }

    }


    return unlockHead;
}

void RangeList::print()
{
    auto node = head;
    while (node != NULL) {
        std::cout << node->toString() << std::endl;
        node = node->forward[0];
    }
}

void RangeList::unlockInsert(RangeList::LockNode *node)
{
    node->unlockForward = unlockHead->unlockForward;
    node->unlockBackward = unlockHead;
    if (node->unlockForward)
        node->unlockForward->unlockBackward = node;
    unlockHead->unlockForward = node;
}

void RangeList::unlockRemove(RangeList::LockNode *node)
{
    if (node->unlockBackward)
        node->unlockBackward->unlockForward = node->unlockForward;
    if (node->unlockForward)
        node->unlockForward->unlockBackward = node->unlockBackward;
}

void RangeList::printUnlock()
{
    auto node = unlockHead;
    while (node != NULL) {
        std::cout << node->toString() << std::endl;
        node = node->unlockForward;
    }
}

void RangeList::push(WireFormat::MigrationIsLocked::Range &range)
{
    std::vector<LockNode *> update;
    LockNode *node = find(range.start, update);
    LockNode *postNode = node->forward[0];

    if (node != BEGIN && node->end + 1 == range.start) {
        node->end = range.end;
        if (postNode != END && range.end + 1 == postNode->start) {
            node->end = postNode->end;
            remove(postNode);
        }
    } else if (postNode != END && range.end + 1 == postNode->start) {
        postNode->start = range.start;
    } else {
        insert(update, new LockNode(range.start, range.end, randomLevel()));
    }
}

bool RangeList::isLocked(uint64_t hash)
{
    std::vector<LockNode *> update;
    LockNode *node = find(hash, update);
    if (node == NULL)
        return false;
    node = node->forward[0];
    if (node == NULL)
        return false;
    return !(node->start <= hash && hash <= node->end);

}


}
