#pragma once

#include <atomic>
#include <vector>
#include <type_traits>
#include <cstring>
#include <string>
#include <stdexcept>

#ifndef SQUERREL_ASSERT
#define SQUERREL_ASSERT(condition, msg, line) if (!(condition)) {throw std::logic_error(std::to_string(line) + ": " + (msg)); }
#endif
#ifndef SQUERREL_EXPECTS
#define SQUERREL_EXPECTS(condition, msg) if (!(condition)) { throw std::invalid_argument(msg); }
#endif

namespace squerrel {
template <size_t MaxElementsNum, size_t DataBufferSize, typename Atom = uint64_t, size_t CacheLineSize = 64>
class queue {
    template <size_t N>
    static constexpr bool is_pow_of_2() { return (N != 0) && ((N & (N - 1)) == 0); }
    static_assert(is_pow_of_2<MaxElementsNum>(), "MaxElementsNum is not a power of two");
    static_assert(is_pow_of_2<DataBufferSize>(), "DataBufferSize is not a power of two");
    static_assert(std::is_unsigned<Atom>::value, "Atom is not an unsigned integer type");
    
    template <size_t N, typename std::enable_if<(N == 1), std::nullptr_t>::type = nullptr>
    static constexpr size_t log2() { return 0; }
    template <size_t N, typename std::enable_if<(N  > 1), std::nullptr_t>::type = nullptr>
    static constexpr size_t log2() { return 1 + log2<N / 2>(); }
    template <size_t N, typename R, typename P>
    static constexpr R mod(P lhs) { return static_cast<R>(lhs & (N - 1)); }

    static constexpr size_t atom_bits = sizeof(Atom) * 8;

    template<Atom N> static bool wrapped_less_than(Atom lhs, Atom rhs) {
        auto diff = mod<N * 2, Atom>(rhs - lhs);
        return (diff <= N) && diff;
    }

    struct state {
        static constexpr Atom nil = 0;
        static constexpr Atom val = 1;
    };

    template<size_t StartBit, size_t EndBit>
    struct bit_slice {
        static constexpr size_t start_bit = StartBit;
        static constexpr size_t end_bit = EndBit;
        static_assert(end_bit <= atom_bits && start_bit < atom_bits && start_bit < end_bit, "Not enough bits in Atom type");
        static constexpr Atom mask = ((Atom(1) << (EndBit - StartBit)) - 1) << StartBit;
        static constexpr Atom encode(Atom arg) { return (arg << start_bit) & mask; }
        static constexpr Atom decode(Atom arg) { return (arg & mask) >> start_bit; }
    };

    struct meta_info {
        Atom begin;
        Atom size;
        Atom state;
        Atom odd_round;
        using begin_slice = bit_slice<0, queue::log2<DataBufferSize>() + 1 >;
        using size_slice = bit_slice<queue::log2<DataBufferSize>() + 1, atom_bits - 2>;
        using state_slice = bit_slice<atom_bits - 2, atom_bits - 1>;
        using odd_round_slice = bit_slice<atom_bits - 1, atom_bits>;
    };

    template <Atom State, typename = void> struct slider {};
    template <Atom State> struct slider<State, typename std::enable_if<State == state::nil>::type> {
        Atom meta_idx;
        Atom data_idx;
        using meta_idx_slice = bit_slice<0, atom_bits - (queue::log2<DataBufferSize>() + 1)>;
        using data_idx_slice = bit_slice<atom_bits - (queue::log2<DataBufferSize>() + 1), atom_bits>;
        bool operator !=(const slider& rhs)const { return (meta_idx != rhs.meta_idx) || (data_idx != rhs.data_idx); }
        void shift(const meta_info& m) {
            data_idx = m.begin + m.size + DataBufferSize;
            ++meta_idx;
        }
        bool less_than(const slider& rhs) const {
            return 
                wrapped_less_than<MaxElementsNum>(meta_idx, rhs.meta_idx) && 
                wrapped_less_than<DataBufferSize>(data_idx, rhs.data_idx);
        }
    };
    template <Atom State> struct slider<State, typename std::enable_if<State == state::val>::type> {
        Atom meta_idx;
        bool operator !=(const slider& rhs)const { return meta_idx != rhs.meta_idx; }
        void shift(const meta_info&) { ++meta_idx; }
        bool less_than(const slider& rhs) const { return wrapped_less_than<MaxElementsNum>(meta_idx, rhs.meta_idx); }
    };

    static Atom pack(const meta_info& src) {
        return meta_info::begin_slice::encode(src.begin) | meta_info::size_slice::encode(src.size) |
               meta_info::state_slice::encode(src.state) | meta_info::odd_round_slice::encode(src.odd_round);
    }
    static Atom pack(const slider<state::nil>& src) {
        return 
            slider<state::nil>::meta_idx_slice::encode(src.meta_idx) |
            slider<state::nil>::data_idx_slice::encode(src.data_idx);
    }
    static Atom pack(const slider<state::val>& src) { return src.meta_idx; }

    template<typename T>
    static typename std::enable_if<std::is_same<meta_info, T>::value, meta_info>::type unpack(Atom src) {
        return { 
            meta_info::begin_slice::decode(src), meta_info::size_slice::decode(src),
            meta_info::state_slice::decode(src), meta_info::odd_round_slice::decode(src)
        };
    }
    template<typename T>
    static typename std::enable_if<std::is_same<slider<state::nil>, T>::value, slider<state::nil>>::type unpack(Atom src) {
        return {
            slider<state::nil>::meta_idx_slice::decode(src),
            slider<state::nil>::data_idx_slice::decode(src)
        };
    }
    template<typename T>
    static typename std::enable_if<std::is_same<slider<state::val>, T>::value, slider<state::val>>::type unpack(Atom src) {
        return { src };
    }
    template<typename T> static T load(const std::atomic<Atom>& atom) {
        return unpack<T>(atom.load(std::memory_order_acquire));
    }
    template<typename T> static void store(std::atomic<Atom>& atom, const T& val) {
        atom.store(pack(val), std::memory_order_release);
    }
    template <typename T> static bool compare_exchange(std::atomic<Atom>& atom, T& expected, const T& desired) {
        Atom packed = pack(expected);
        if (!atom.compare_exchange_strong(packed, pack(desired), std::memory_order_release, std::memory_order_acquire)) {
            expected = unpack<T>(packed);
            return false;
        }
        return true;
    }

    template<Atom State> slider<State> slide_forward(std::atomic<Atom>& sl) {
        auto expected = load<slider<State>>(sl);
        auto desired = expected;

        for (size_t i = 0; i < slide_limit; ++i) {
            Atom odd_round = (State == state::val) == !(desired.meta_idx & MaxElementsNum);
            auto m = load<meta_info>(meta_buf[mod<MaxElementsNum, size_t>(desired.meta_idx)]);
            if (m.state != State || m.odd_round != odd_round) {
                break;
            }
            desired.shift(m);
        }
        return ((expected != desired) && !compare_exchange(sl, expected, desired)) ? expected : desired;
    }

    void read_data(size_t idx, char* dst, size_t size) {
        size_t s = std::min(size, DataBufferSize - idx);
        memcpy(dst, &data_buf[idx], s);
        memcpy(dst + s, &data_buf[0], size - s);
    }
    void write_data(size_t idx, const char* src, size_t size) {
        size_t s = std::min(size, DataBufferSize - idx);
        memcpy(&data_buf[idx], src, s);
        memcpy(&data_buf[0], src + s, size - s);
    }
    size_t consume_data(size_t meta_idx, void* dst, size_t capacity) {
        meta_info m = load<meta_info>(meta_buf[mod<MaxElementsNum, size_t>(meta_idx)]);

        SQUERREL_ASSERT(m.state == state::val, "No value, idx = " + std::to_string(meta_idx), __LINE__)
        SQUERREL_EXPECTS(m.size <= capacity, "Capacity " + std::to_string(capacity) +
            " isn't sufficient to accommodate an element of size " + std::to_string(m.size));

        read_data(mod<DataBufferSize, size_t>(m.begin), static_cast<char*>(dst), m.size);
        m.state = state::nil;
        store(meta_buf[mod<MaxElementsNum, size_t>(meta_idx)], m);
        return m.size;
    }

    const size_t slide_limit;
    alignas(CacheLineSize) std::vector<std::atomic<Atom> > meta_buf;
    alignas(CacheLineSize) std::vector<char> data_buf;

    alignas(CacheLineSize) std::atomic<Atom> nil_begin{};
    alignas(CacheLineSize) std::atomic<Atom> nil_end{
        pack(slider<state::nil>{static_cast<Atom>(MaxElementsNum), static_cast<Atom>(DataBufferSize)})
    };
    alignas(CacheLineSize) std::atomic<Atom> val_begin{};
    alignas(CacheLineSize) std::atomic<Atom> val_end{};

    static constexpr size_t bitwise_max_size =
        (static_cast<Atom>(1) << (meta_info::size_slice::end_bit - meta_info::size_slice::start_bit)) - static_cast<Atom>(1);
public:
    static constexpr size_t max_size = DataBufferSize < bitwise_max_size ? DataBufferSize : bitwise_max_size;

    explicit queue(size_t slide_limit = static_cast<size_t>(-1))
        : slide_limit(slide_limit), meta_buf(MaxElementsNum), data_buf(DataBufferSize, 0) {
        for (auto& m : meta_buf) {
            m.store(0, std::memory_order_release);
        }
    }
    size_t try_pop(void* dst, size_t capacity) {
        auto cur = load<slider<state::val>>(val_begin);
        auto end = slide_forward<state::val>(val_end);
        for (;;) {
            if (!cur.less_than(end)) {
                return 0;
            }
            if (compare_exchange(val_begin, cur, slider<state::val>{ static_cast<Atom>(cur.meta_idx + 1) })) {
                return consume_data(cur.meta_idx, dst, capacity);
            }
            end = load<slider<state::val>>(val_end);
        }
    }

    class bulk {
        friend queue;
        Atom cur;
        Atom end;
        bulk(Atom cur, Atom end) : cur(cur), end(end) {}
    public:
        bool empty()const { return cur == end; }
    };

    bulk pop_bulk() {
        auto cur = load<slider<state::val>>(val_begin);
        auto end = slide_forward<state::val>(val_end);
        for (;;) {
            if (!cur.less_than(end)) {
                return { {}, {} };
            }
            if (compare_exchange(val_begin, cur, end)) {
                return { cur.meta_idx, end.meta_idx };
            }
            end = load<slider<state::val>>(val_end);
        }
    }

    size_t consume_from_bulk(bulk& b, void* dst, size_t capacity) {
        return b.empty() ? size_t{} : consume_data(b.cur++, dst, capacity);
    }

    bool try_push(const void* src, size_t size) {
        SQUERREL_EXPECTS(size && (size <= max_size), "Element size (" + std::to_string(size) + 
            ") must be greater than 0 and less than or equal to " + std::to_string(max_size));
        auto cur = load<slider<state::nil>>(nil_begin);
        auto end = slide_forward<state::nil>(nil_end);
        for (;;) {
            if (!slider<state::nil>{cur.meta_idx, static_cast<Atom>(cur.data_idx + (size - 1)) }.less_than(end)) {
                return false;
            }
            if (compare_exchange(nil_begin, cur,
                slider<state::nil>{ static_cast<Atom>(cur.meta_idx + 1), static_cast<Atom>(cur.data_idx + size) })) {
                break;
            }
            end = load<slider<state::nil>>(nil_end);
        }
        auto m = load<meta_info>(meta_buf[mod<MaxElementsNum, size_t>(cur.meta_idx)]);

        SQUERREL_ASSERT(m.state == state::nil, "No space, idx = " + std::to_string(cur.meta_idx), __LINE__)
        
        write_data(mod<DataBufferSize, size_t>(cur.data_idx), static_cast<const char*>(src), size);
        store(meta_buf[mod<MaxElementsNum, size_t>(cur.meta_idx)],
            meta_info{ cur.data_idx, static_cast<Atom>(size), state::val, !(cur.meta_idx & MaxElementsNum) });
        return true;
    }
};
template <size_t MaxElementsNum, size_t DataBufferSize, typename Atom, size_t CacheLineSize>
constexpr size_t queue<MaxElementsNum, DataBufferSize, Atom, CacheLineSize>::max_size;
}