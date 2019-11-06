#ifndef RAMCLOUD_COMMQUEUE_H
#define RAMCLOUD_COMMQUEUE_H

#include <list>
#include <boost/lockfree/spsc_queue.hpp>

#include "SpinLock.h"

namespace RAMCloud {

template<typename T>
class QueueWithSpinLock {
  public:

    QueueWithSpinLock(int size)
        : l(), lock("QueueWithSpinLock")
    {
    }

    void push(T e)
    {
        SpinLock::Guard guard(lock);
        l.push_back(e);
    }

    T &front()
    {
        SpinLock::Guard guard(lock);
        return l.front();
    }

    void pop()
    {
        SpinLock::Guard guard(lock);
        l.pop_front();
    }

    bool empty()
    {
        SpinLock::Guard guard(lock);
        return l.empty();
    }

  private:
    std::list<T> l;
    SpinLock lock;

};

}

#define CommQueue boost::lockfree::spsc_queue

#endif //RAMCLOUD_COMMQUEUE_H
