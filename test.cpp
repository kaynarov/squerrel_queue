#include "queue.hpp"

#include <iostream>
#include <thread>
#include <array>
#include <random>

using buffer = std::array<char, 19>;

thread_local std::random_device rand_dev;
thread_local std::mt19937 rand_gen(rand_dev());
std::uniform_int_distribution<uint64_t> digit_distrib(0, 9);

template<typename Queue>
struct squerrel_producer {
    Queue& queue;
    buffer buf{};
    uint64_t sum{};
    std::uniform_int_distribution<size_t> len_distrib;
    uint64_t left;
    squerrel_producer(Queue& queue, uint64_t vals_num) 
        : queue(queue)
        , len_distrib(1, std::min(buf.size(), Queue::max_size))
        , left(vals_num + 1) {}
    bool operator()() {
        --left;
        uint64_t val{};
        auto len = left ? len_distrib(rand_gen) : 1;
        
        if (left) {
            for (size_t i = 0; i < len; i++) {
                val *= 10;
                auto d = i ? digit_distrib(rand_gen) : std::max<uint64_t>(digit_distrib(rand_gen), 1);
                val += d;
                buf[i] = static_cast<char>('0' + d);
            }
            sum += val;
        }
        else {
            buf[0] = '0';
        }
        while (!queue.try_push(buf.data(), len)) {
            std::this_thread::yield();
        }
        return !left;
    }
};

uint64_t read_val(const buffer& buf, size_t len) {
    uint64_t val{};
    for (size_t i = 0; i < len; i++) {
        val *= 10;
        val += static_cast<uint64_t>(buf[i]) - '0';
    }
    return val;
}

template<typename Queue>
struct squerrel_consumer {
    Queue& queue;
    buffer buf{};
    uint64_t sum{};
    squerrel_consumer(Queue& queue) : queue(queue) {}
    size_t operator()() {
        if (size_t len = queue.try_pop(buf.data(), buf.size())) {
            uint64_t val = read_val(buf, len);
            if (!val) {
                return 1;
            }
            sum += val;
        }
        return 0;
    }
};

template<typename Queue>
struct squerrel_bulk_consumer {
    Queue& queue;
    buffer buf{};
    uint64_t sum{};
    squerrel_bulk_consumer(Queue& queue) : queue(queue) {}
    size_t operator()() {
        auto bulk = queue.pop_bulk();
        size_t done{};
        while (!bulk.empty()) {
            if (size_t len = queue.consume_from_bulk(bulk, buf.data(), buf.size())) {
                uint64_t val = read_val(buf, len);
                done += val ? 0 : 1;
                sum += val;
            }
            else {
                break;
            }
        }
        return done;
    }
};

template <typename Queue, bool Bulk> struct consumer { using type = squerrel_consumer<Queue>; };
template <typename Queue> struct consumer<Queue, true> { using type = squerrel_bulk_consumer<Queue>; };

template <size_t MaxElementsNum, size_t DataBufferSize, typename Atom, bool Bulk>
void test(size_t threads_num, uint64_t vals_num, size_t slide_limit) {
    size_t consumers_num = std::max<size_t>(1, threads_num / (Bulk ? 3 : 2));
    size_t producers_num = threads_num - consumers_num;

    std::cout << "Sending through <" << MaxElementsNum << "/" << DataBufferSize << "/" << sizeof(Atom) * 8 << "> "
        << producers_num << ">>>" << consumers_num << (Bulk ? " bulk" : "") << "...";

    using squerrel_queue = squerrel::queue<MaxElementsNum, DataBufferSize, Atom>;

    squerrel_queue queue(slide_limit);
    std::atomic<size_t> streams_left{ producers_num };

    using prod = squerrel_producer<squerrel_queue>;
    using cons = typename consumer<squerrel_queue, Bulk>::type;
    std::vector<prod> producers(producers_num,prod(queue, vals_num / producers_num));
    std::vector<cons> consumers(consumers_num, cons(queue));

    std::vector<std::thread> threads;
    for (auto& producer: producers) {
        threads.emplace_back(std::thread([&producer]() {while (!producer()); }));
    }
    for (auto& consumer: consumers) {
        threads.emplace_back(std::thread([&consumer, &streams_left] {
                while (streams_left.load(std::memory_order_relaxed)) {
                    if (size_t n = consumer()) {
                        streams_left.fetch_sub(n, std::memory_order_acq_rel);
                    }
                }
            }
        ));
    }

    for (auto& t : threads) {
        t.join();
    }

    uint64_t produced_sum{};
    for (auto& producer : producers) {
        produced_sum += producer.sum;
    }
    uint64_t consumed_sum{};
    for (auto& consumer : consumers) {
        consumed_sum += consumer.sum;
    }

    if (produced_sum != consumed_sum) {
        std::cout << " FAILURE! (produced sum = " << produced_sum << ", consumed sum = " << consumed_sum << ")\n";
        std::exit(EXIT_FAILURE);
    }
    else {
        std::cout << " OK (transferred sum = " << produced_sum << ")\n";
    }
}

int main(int argc, char* argv[]) {

    uint64_t vals_num = (argc > 1) ? std::stoull(argv[1]) : static_cast<uint64_t>(1024 * 1024);
    size_t slide_limit = (argc > 2) ? std::stoull(argv[2]) : static_cast<size_t>(-1);

    test<1024, 16384, uint64_t, false>(8, vals_num, slide_limit);
    test<1024, 16384, uint64_t, true>(8, vals_num, slide_limit);
    test<1024, 8, uint64_t, false>(8, vals_num, slide_limit);
    test<4, 256, uint64_t, true>(8, vals_num, slide_limit);

    test<512, 4096, uint32_t, false>(8, vals_num, slide_limit);
    test<512, 4096, uint32_t, true>(8, vals_num, slide_limit);

    test<4, 8, uint8_t, false>(2, vals_num, slide_limit);
    test<4, 8, uint8_t, true>(2, vals_num, slide_limit);

    return 0;
}