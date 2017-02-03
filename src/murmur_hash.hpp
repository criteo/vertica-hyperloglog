#ifndef _MURMUR_HASH_H_
#define _MURMUR_HASH_H_

template <typename T>
class Hash {
  public:
    virtual uint64_t operator()(T value) const = 0;
};

template<typename T>
class MurMurHash : public Hash<T> {};

template<>
class MurMurHash<uint64_t> : public Hash<uint64_t>{
  public:
    uint64_t operator()(uint64_t value) const override {
      const uint32_t HASH_SEED = 27072015;
      const uint64_t m = 0xc6a4a7935bd1e995;
      unsigned int seed = HASH_SEED;
      const int r = 47;
      uint64_t h = seed ^ (sizeof(uint64_t) * m);
      uint64_t k = value;
      k *= m;
      k ^= k >> r;
      k *= m;
      h ^= k;
      h *= m;
      h *= m;
      h ^= h >> r;
      h *= m;
      h ^= h >> r;
      return h;
    }
};

template<>
class MurMurHash<uint32_t> : public Hash<uint32_t>{
  public:
    uint64_t operator()(uint32_t value) const override {
      return MurMurHash<uint64_t>()(static_cast<uint64_t>(value));
    }
};

#endif