#pragma once

#include <Common/Arena.h>
#include <Common/HashTable/HashTable.h>

namespace DB
{

/**
  * ArenaKeyPtr is a key holder for hash tables that serializes the key to arena.
  * For types other than StringRef, it does nothing, because they are stored
  * by value.
  * StringRef is also technically stored by value inside a hash table, but it is
  * a pointer type. We copy the data it points to to the arena, and update the
  * pointer.
  */
template <typename Key>
struct ArenaKeyPtr : public NoopKeyPtr<Key>
{
    ArenaKeyPtr(Key key_, Arena &) : NoopKeyPtr<Key>(key_) {}
};

template <>
struct ArenaKeyPtr<StringRef>
{
    StringRef key;
    Arena & pool;

    ArenaKeyPtr(StringRef key_, Arena & pool_)
        : key(key_), pool(pool_) {}

    StringRef & persist()
    {
        if (key.size)
        {
            key.data = pool.insert(key.data, key.size);
        }
        return key;
    }

    StringRef & operator * ()
    {
        return key;
    }
};

/**
  * SerializedKeyPtr is a key holder for a key that is already serialized to an
  * arena. The key must be the last serialized key for this arena.
  * In destructor, if the key was made persistent, we do nothing, and if not --
  * roll back the last allocation using Arena::rollback().
  */
struct SerializedKeyPtr
{
    StringRef key;
    Arena * pool;

    SerializedKeyPtr(StringRef key_, Arena & pool_) : key(key_), pool(&pool_) {}

    StringRef & persist()
    {
        assert(pool != nullptr);
        pool = nullptr;
        return key;
    }

    StringRef & operator * ()
    {
        return key;
    }

    ~SerializedKeyPtr()
    {
        if (pool != nullptr)
        {
            [[maybe_unused]] void * new_head = pool->rollback(key.size);
            assert(new_head == key.data);
        }
    }

    SerializedKeyPtr(SerializedKeyPtr && other)
    {
        key = other.key;
        pool = other.pool;
        other.pool = nullptr;
    }

private:
    SerializedKeyPtr(const SerializedKeyPtr &) = delete;
    SerializedKeyPtr operator = (const SerializedKeyPtr &) = delete;
};

} // namespace DB

