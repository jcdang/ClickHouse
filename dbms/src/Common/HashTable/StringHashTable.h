#pragma once

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTable.h>

/// TODO feature macros

#define CASE_1_8 \
    case 1: \
    case 2: \
    case 3: \
    case 4: \
    case 5: \
    case 6: \
    case 7: \
    case 8

#define CASE_9_16 \
    case 9: \
    case 10: \
    case 11: \
    case 12: \
    case 13: \
    case 14: \
    case 15: \
    case 16

#define CASE_17_24 \
    case 17: \
    case 18: \
    case 19: \
    case 20: \
    case 21: \
    case 22: \
    case 23: \
    case 24

struct StringKey0
{
};

using StringKey8 = UInt64;
using StringKey16 = DB::UInt128;
struct StringKey24
{
    UInt64 a;
    UInt64 b;
    UInt64 c;

    bool operator==(const StringKey24 rhs) const { return a == rhs.a && b == rhs.b && c == rhs.c; }
    bool operator!=(const StringKey24 rhs) const { return !operator==(rhs); }
    bool operator==(const UInt64 rhs) const { return a == rhs && b == 0 && c == 0; }
    bool operator!=(const UInt64 rhs) const { return !operator==(rhs); }

    StringKey24 & operator=(const UInt64 rhs)
    {
        a = rhs;
        b = 0;
        c = 0;
        return *this;
    }
};

inline StringRef ALWAYS_INLINE toStringRef(const StringKey8 & n)
{
    return {reinterpret_cast<const char *>(&n), 8ul - (__builtin_clzll(n) >> 3)};
}
inline StringRef ALWAYS_INLINE toStringRef(const StringKey16 & n)
{
    return {reinterpret_cast<const char *>(&n), 16ul - (__builtin_clzll(n.high) >> 3)};
}
inline StringRef ALWAYS_INLINE toStringRef(const StringKey24 & n)
{
    return {reinterpret_cast<const char *>(&n), 24ul - (__builtin_clzll(n.c) >> 3)};
}
inline const StringRef & ALWAYS_INLINE toStringRef(const StringRef & s)
{
    return s;
}

struct StringHashTableHash
{
    size_t ALWAYS_INLINE operator()(StringKey8 key) const
    {
        size_t res = -1ULL;
        res = _mm_crc32_u64(res, key);
        return res;
    }
    size_t ALWAYS_INLINE operator()(StringKey16 key) const
    {
        size_t res = -1ULL;
        res = _mm_crc32_u64(res, key.low);
        res = _mm_crc32_u64(res, key.high);
        return res;
    }
    size_t ALWAYS_INLINE operator()(StringKey24 key) const
    {
        size_t res = -1ULL;
        res = _mm_crc32_u64(res, key.a);
        res = _mm_crc32_u64(res, key.b);
        res = _mm_crc32_u64(res, key.c);
        return res;
    }
    size_t ALWAYS_INLINE operator()(StringRef key) const
    {
        size_t res = -1ULL;
        size_t sz = key.size;
        const char * p = key.data;
        const char * lp = p + sz - 8; // starting pointer of the last 8 bytes segment
        char s = (-sz & 7) * 8; // pending bits that needs to be shifted out
        UInt64 n[3]; // StringRef in SSO map will have length > 24
        memcpy(&n, p, 24);
        res = _mm_crc32_u64(res, n[0]);
        res = _mm_crc32_u64(res, n[1]);
        res = _mm_crc32_u64(res, n[2]);
        p += 24;
        while (p + 8 < lp)
        {
            memcpy(&n[0], p, 8);
            res = _mm_crc32_u64(res, n[0]);
            p += 8;
        }
        memcpy(&n[0], lp, 8);
        n[0] >>= s;
        res = _mm_crc32_u64(res, n[0]);
        return res;
    }
};

template <typename Cell>
struct StringHashTableEmpty
{
    using Self = StringHashTableEmpty;
    using MappedPtr = typename Cell::mapped_type *;

    Cell value;
    bool is_empty{true};

    StringHashTableEmpty() { memset(reinterpret_cast<char *>(&value), 0, sizeof(value)); }

    template <typename KeyPtr>
    void ALWAYS_INLINE emplacePtr(KeyPtr && key_ptr, MappedPtr & it,
                               bool & inserted, size_t hash)
    {
        emplace(*key_ptr, it, inserted, hash);
    }

    template <typename Key>
    void ALWAYS_INLINE emplace(Key, MappedPtr & it, bool & inserted, size_t)
    {
        if (is_empty)
        {
            inserted = true;
            is_empty = false;
        }
        else
            inserted = false;

        it = value.getMapped();
    }

    template <typename Key>
    MappedPtr ALWAYS_INLINE find(Key, size_t)
    {
        return value.getMapped();
    }

    void write(DB::WriteBuffer & wb) const { value.write(wb); }
    void writeText(DB::WriteBuffer & wb) const { value.writeText(wb); }
    void read(DB::ReadBuffer & rb) { value.read(rb); }
    void readText(DB::ReadBuffer & rb) { value.readText(rb); }
    size_t size() const { return is_empty ? 0 : 1; }
    bool empty() const { return is_empty; }
    size_t getBufferSizeInBytes() const { return sizeof(Cell); }
    size_t getCollisions() const { return 0; }
};

template <size_t initial_size_degree = 8>
struct StringHashTableGrower : public HashTableGrower<initial_size_degree>
{
    // Smooth growing for string maps
    void increaseSize() { this->size_degree += 1; }
};

template <typename SubMaps>
class StringHashTable : private boost::noncopyable
{
protected:
    static constexpr size_t NUM_MAPS = 5;
    // Map for storing empty string
    using T0 = typename SubMaps::T0;

    // Short strings are stored as numbers
    using T1 = typename SubMaps::T1;
    using T2 = typename SubMaps::T2;
    using T3 = typename SubMaps::T3;

    // Long strings are stored as StringRef along with saved hash
    using Ts = typename SubMaps::Ts;
    using Self = StringHashTable;

    template <typename, typename, size_t>
    friend class TwoLevelStringHashTable;

    T0 m0;
    T1 m1;
    T2 m2;
    T3 m3;
    Ts ms;

public:
    using Key = StringRef;
    using key_type = Key;
    using value_type = typename Ts::value_type;
    using MappedPtr = typename Ts::MappedPtr;

    StringHashTable() {}

    StringHashTable(size_t reserve_for_num_elements)
        : m1{reserve_for_num_elements / 4}
        , m2{reserve_for_num_elements / 4}
        , m3{reserve_for_num_elements / 4}
        , ms{reserve_for_num_elements / 4}
    {
    }

    StringHashTable(StringHashTable && rhs) { *this = std::move(rhs); }
    ~StringHashTable() {}

public:
    // Dispatch is written in a way that maximizes the performance:
    // 1. Always memcpy 8 times bytes
    // 2. Use switch case extension to generate fast dispatching table
    // 3. Combine hash computation along with key loading
    // 4. Funcs are named callables that can be force_inlined
    // NOTE: It relies on Little Endianness and SSE4.2
    template <typename Func, typename KeyPtr>
    decltype(auto) ALWAYS_INLINE dispatch(KeyPtr && key_ptr, Func && func)
    {
        static constexpr StringKey0 key0{};
        const StringRef & x = *key_ptr;
        size_t sz = x.size;
        const char * p = x.data;
        // pending bits that needs to be shifted out
        char s = (-sz & 7) * 8;
        size_t res = -1ULL;
        union
        {
            StringKey8 k8;
            StringKey16 k16;
            StringKey24 k24;
            UInt64 n[3];
        };
        switch (sz)
        {
            case 0:
                return func(m0, NoopKeyPtr(key0), 0);
            CASE_1_8 : {
                // first half page
                if ((reinterpret_cast<uintptr_t>(p) & 2048) == 0)
                {
                    memcpy(&n[0], p, 8);
                    n[0] &= -1ul >> s;
                }
                else
                {
                    const char * lp = x.data + x.size - 8;
                    memcpy(&n[0], lp, 8);
                    n[0] >>= s;
                }
                res = _mm_crc32_u64(res, n[0]);
                return func(m1, NoopKeyPtr(k8), res);
            }
            CASE_9_16 : {
                memcpy(&n[0], p, 8);
                res = _mm_crc32_u64(res, n[0]);
                const char * lp = x.data + x.size - 8;
                memcpy(&n[1], lp, 8);
                n[1] >>= s;
                res = _mm_crc32_u64(res, n[1]);
                return func(m2, NoopKeyPtr(k16), res);
            }
            CASE_17_24 : {
                memcpy(&n[0], p, 16);
                res = _mm_crc32_u64(res, n[0]);
                res = _mm_crc32_u64(res, n[1]);
                const char * lp = x.data + x.size - 8;
                memcpy(&n[2], lp, 8);
                n[2] >>= s;
                res = _mm_crc32_u64(res, n[2]);
                return func(m3, NoopKeyPtr(k24), res);
            }
            default: {
                memcpy(&n, x.data, 24);
                res = _mm_crc32_u64(res, n[0]);
                res = _mm_crc32_u64(res, n[1]);
                res = _mm_crc32_u64(res, n[2]);
                p += 24;
                const char * lp = x.data + x.size - 8;
                while (p + 8 < lp)
                {
                    memcpy(&n[0], p, 8);
                    res = _mm_crc32_u64(res, n[0]);
                    p += 8;
                }
                memcpy(&n[0], lp, 8);
                n[0] >>= s;
                res = _mm_crc32_u64(res, n[0]);
                return func(ms, key_ptr, res);
            }
        }
    }

    struct EmplaceCallable
    {
        MappedPtr & mapped;
        bool & inserted;

        EmplaceCallable(MappedPtr & mapped_, bool & inserted_)
            : mapped(mapped_), inserted(inserted_) {}

        template <typename Map, typename KeyPtr>
        void ALWAYS_INLINE operator()(Map & map, KeyPtr && key_ptr, size_t hash)
        {
            map.emplacePtr(key_ptr, mapped, inserted, hash);
        }
    };

    template <typename KeyPtr>
    void ALWAYS_INLINE emplacePtr(KeyPtr && key_ptr, MappedPtr & it, bool & inserted)
    {
        this->dispatch(key_ptr, EmplaceCallable(it, inserted));
    }

    void ALWAYS_INLINE emplace(Key & key, MappedPtr & it, bool & inserted)
    {
        emplacePtr(NoopKeyPtr(key), it, inserted);
    }

    struct FindCallable
    {
        template <typename Map, typename KeyPtr>
        MappedPtr ALWAYS_INLINE operator()(Map & map, KeyPtr && key_ptr, size_t hash)
        {
            return map.find(*key_ptr, hash);
        }
    };

    MappedPtr ALWAYS_INLINE find(Key x)
    {
        return dispatch(NoopKeyPtr(x), FindCallable{});
    }

    void write(DB::WriteBuffer & wb) const
    {
        m0.write(wb);
        m1.write(wb);
        m2.write(wb);
        m3.write(wb);
        ms.write(wb);
    }

    void writeText(DB::WriteBuffer & wb) const
    {
        m0.writeText(wb);
        DB::writeChar(',', wb);
        m1.writeText(wb);
        DB::writeChar(',', wb);
        m2.writeText(wb);
        DB::writeChar(',', wb);
        m3.writeText(wb);
        DB::writeChar(',', wb);
        ms.writeText(wb);
    }

    void read(DB::ReadBuffer & rb)
    {
        m0.read(rb);
        m1.read(rb);
        m2.read(rb);
        m3.read(rb);
        ms.read(rb);
    }

    void readText(DB::ReadBuffer & rb)
    {
        m0.readText(rb);
        DB::assertChar(',', rb);
        m1.readText(rb);
        DB::assertChar(',', rb);
        m2.readText(rb);
        DB::assertChar(',', rb);
        m3.readText(rb);
        DB::assertChar(',', rb);
        ms.readText(rb);
    }

    size_t size() const { return m0.size() + m1.size() + m2.size() + m3.size() + ms.size(); }

    bool empty() const { return m0.empty() && m1.empty() && m2.empty() && m3.empty() && ms.empty(); }

    size_t getBufferSizeInBytes() const
    {
        return m0.getBufferSizeInBytes() + m1.getBufferSizeInBytes() + m2.getBufferSizeInBytes() + m3.getBufferSizeInBytes()
            + ms.getBufferSizeInBytes();
    }

    void clearAndShrink()
    {
        using Cell = decltype(m0.value);
        if (!std::is_trivially_destructible_v<Cell>)
            m0.value.~Cell();
        m1.clearAndShrink();
        m2.clearAndShrink();
        m3.clearAndShrink();
        ms.clearAndShrink();
    }
};
