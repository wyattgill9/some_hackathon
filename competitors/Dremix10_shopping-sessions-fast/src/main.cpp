// Fast path.
//
//   phase 1  SIMD-indexed CSV parse. Events become 32-byte records scattered
//            by hash(event_id); clicks become 24-byte records scattered by
//            hash(person). Links are resolved on one thread meanwhile.
//   phase 2  Per event_id partition: dedup in a cache-sized table, resolve the
//            person, scatter 16-byte (person, ts, purchase) records by person.
//   phase 3  Per person partition: group, sort, sessionize, attribute, and
//            append rows to per-thread Parquet column buffers.
//
// Every input the compact formats cannot hold exactly (see bail()) is handed
// to the generic pipeline in fallback.cpp instead.
#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <dirent.h>
#include <fcntl.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>
#if defined(__x86_64__)
#include <immintrin.h>
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif

using i64 = int64_t;
using u64 = uint64_t;
using u32 = uint32_t;
using u16 = uint16_t;
using u8 = uint8_t;
using std::string;
using std::vector;

int fallback_main(const std::string& data, const std::string& outdir);
#ifdef PGO_GEN
extern "C" void __gcov_dump(void);
#endif

#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#define NOINLINE __attribute__((noinline))
#define AINLINE inline __attribute__((always_inline))

static const i64 DAYMS = 86400000;
static const u32 GAP_MS = 1800000;
static const u32 SEVEN_MS = 604800000;
static const u32 TS_INVALID = 0xFFFFFFFFu;
// ingest_ts is kept as a key that orders like the time itself: the UTC day
// counted from 64 days before the window's first day, then HHMMSS as six
// decimal digits of four bits each.
static const i64 INGEST_DAY_BIAS = 64;

static int NT = 8;
static bool VERBOSE = false;
static auto T0 = std::chrono::steady_clock::now();
static double now_ms() {
  return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - T0).count();
}
static void tick(const char* what) {
  if (VERBOSE) fprintf(stderr, "[%8.1f ms] %s\n", now_ms(), what);
}

static i64 WS, WE;      // event window, epoch ms
static u64 WLEN;        // WE - WS
static u64 CWLEN;       // WE - (WS - 7 days)
static i64 DAY0;        // epoch day of WS
static u32 WS_TOD;      // WS - DAY0 * DAYMS
static int ND;          // days the window touches
static std::atomic<bool> g_bail{false};
static u32 g_nbkt = 1;  // 30-minute buckets in the window
static int g_bbits = 1; // bits to hold a bucket number

static AINLINE u64 load64(const char* p) {
  u64 v;
  memcpy(&v, p, 8);
  return v;
}
static AINLINE u32 load32(const char* p) {
  u32 v;
  memcpy(&v, p, 4);
  return v;
}
static AINLINE u64 mix(u64 x) {
  x *= 0x9E3779B97F4A7C15ull;
  x ^= x >> 32;
  x *= 0xD6E8FEB86659FD93ull;
  return x;
}
// One multiply; callers use the top bits of the result.
static AINLINE u64 hash64(u64 x) { return x * 0x9E3779B97F4A7C15ull; }
static AINLINE u32 tz64(u64 m) {  // 64 for zero
#if defined(__BMI__)
  return (u32)_tzcnt_u64(m);
#else
  return (u32)std::countr_zero(m);
#endif
}
static AINLINE u64 mulhi(u64 a, u64 b) { return (u64)(((unsigned __int128)a * b) >> 64); }

static bool g_pin = false;
static void pin_thread(int i) {
#ifdef __linux__
  if (!g_pin) return;
  cpu_set_t all;
  if (sched_getaffinity(0, sizeof all, &all) != 0) return;
  int n = CPU_COUNT(&all), want = i % (n > 0 ? n : 1), seen = 0;
  for (int c = 0; c < CPU_SETSIZE; c++)
    if (CPU_ISSET(c, &all) && seen++ == want) {
      cpu_set_t one;
      CPU_ZERO(&one);
      CPU_SET(c, &one);
      sched_setaffinity(0, sizeof one, &one);
      return;
    }
#else
  (void)i;
#endif
}
// The worker threads, started once and kept: the run has seven short parallel
// steps, and creating and joining seven threads for each of them shows.
struct Pool {
  std::mutex mu;  // guards fn, ctx, n, pending
  std::condition_variable job, done;
  std::mutex one;  // one parallel step at a time
  vector<std::thread> ts;
  void (*fn)(void*, int) = nullptr;
  void* ctx = nullptr;
  int n = 0;
  int pending = 0;
  std::atomic<u64> gen{0};
  void start(int threads) {
    for (int i = 1; i < threads; i++) ts.emplace_back([this, i] { worker(i); });
  }
  void worker(int i) {
    pin_thread(i);
    u64 seen = 0;
    for (;;) {
      // The next step usually follows within microseconds: look for it briefly before sleeping.
      for (int spin = 0; spin < 2000 && gen.load(std::memory_order_acquire) == seen; spin++) {
#if defined(__x86_64__)
        _mm_pause();
#endif
      }
      void (*f)(void*, int);
      void* c;
      int count;
      {
        std::unique_lock<std::mutex> lk(mu);
        job.wait(lk, [&] { return gen.load(std::memory_order_relaxed) != seen; });
        seen = gen.load(std::memory_order_relaxed);
        f = fn, c = ctx, count = n;
      }
      if (i < count) f(c, i);
      {
        std::lock_guard<std::mutex> lk(mu);
        if (--pending == 0) done.notify_one();
      }
    }
  }
  // Runs f(c, 0) here and f(c, i) on worker i for i below count; returns when all have finished.
  void run(int count, void (*f)(void*, int), void* c) {
    std::lock_guard<std::mutex> only(one);
    {
      std::lock_guard<std::mutex> lk(mu);
      fn = f, ctx = c, n = count, pending = (int)ts.size();
      gen.fetch_add(1, std::memory_order_release);
    }
    job.notify_all();
    f(c, 0);
    std::unique_lock<std::mutex> lk(mu);
    done.wait(lk, [&] { return pending == 0; });
  }
};
static Pool g_pool;
// Calls f(0) .. f(n - 1), each on its own thread; n is at most the pool's size.
template <class F>
static void parallel(int n, F&& f) {
  if (n <= 1 || g_pool.ts.empty()) {
    for (int i = 0; i < n; i++) f(i);
    return;
  }
  typedef std::remove_reference_t<F> Fn;
  g_pool.run(n, [](void* c, int i) { (*(Fn*)c)(i); }, (void*)&f);
}

// ---------------------------------------------------------------- IDs
// An ID is a 64-bit key. `<prefix><1..8 non-NUL bytes>` is stored as those
// bytes (little-endian, zero padded): exact, and the generator writes nothing
// else. Any other ID is interned, key = index << 8, so byte 0 tells them apart.
static const u64 LENMASK[9] = {0ull,
                               0xFFull,
                               0xFFFFull,
                               0xFFFFFFull,
                               0xFFFFFFFFull,
                               0xFFFFFFFFFFull,
                               0xFFFFFFFFFFFFull,
                               0xFFFFFFFFFFFFFFull,
                               ~0ull};
#define ONES 0x0101010101010101ull
#define HIGHS 0x8080808080808080ull
static AINLINE bool has_zero_in(u64 w, u32 n) {  // any zero among the low n bytes
  u64 t = w | ~LENMASK[n];
  return ((t - ONES) & ~t & HIGHS) != 0;
}
struct Dict {
  std::mutex mu;
  std::unordered_map<string, u64> map;
  std::deque<string> strs;
  u64 intern(const char* p, u32 n) {
    std::lock_guard<std::mutex> lk(mu);
    string s(p, n);
    auto it = map.find(s);
    if (it != map.end()) return it->second;
    strs.push_back(s);
    u64 k = (u64)strs.size() << 8;
    map.emplace(std::move(s), k);
    return k;
  }
  string str(u64 key) {
    std::lock_guard<std::mutex> lk(mu);
    return strs[(key >> 8) - 1];
  }
};
static Dict g_dict;
static u64 id_key(const char* p, u32 n, char pfx) {  // n > 0
  if (n >= 2 && n <= 9 && p[0] == pfx) {
    u64 k = 0;
    memcpy(&k, p + 1, n - 1);
    if (!has_zero_in(k, n - 1)) return k;
  }
  return g_dict.intern(p, n);
}
static AINLINE bool is_raw(u64 k) { return (k & 0xFF) != 0; }
static AINLINE u32 raw_len(u64 k) { return 8 - ((u32)std::countl_zero(k) >> 3); }
static string id_str(u64 k, char pfx) {
  if (is_raw(k)) {
    char b[9];
    b[0] = pfx;
    memcpy(b + 1, &k, 8);
    return string(b, 1 + raw_len(k));
  }
  return g_dict.str(k);
}
static AINLINE bool id_less(u64 a, u64 b, char pfx) {  // byte-wise order of the IDs
  if (LIKELY(is_raw(a) && is_raw(b))) return __builtin_bswap64(a) < __builtin_bswap64(b);
  return id_str(a, pfx) < id_str(b, pfx);
}

// ---------------------------------------------------------------- generic CSV and time
static inline bool split(const char* s, const char* e, int nf, const char** fp, u32* fl) {
  if (s == e) return false;
  int k = 0;
  const char* p = s;
  for (;;) {
    const char* c = (const char*)memchr(p, ',', e - p);
    const char* fe = c ? c : e;
    if (k >= nf) return false;
    u32 len = (u32)(fe - p);
    if (len > 0 && memchr(p, '"', len)) {
      if (len < 2 || p[0] != '"' || p[len - 1] != '"' || (len > 2 && memchr(p + 1, '"', len - 2))) return false;
      fp[k] = p + 1;
      fl[k] = len - 2;
    } else {
      fp[k] = p;
      fl[k] = len;
    }
    k++;
    if (!c) break;
    p = c + 1;
  }
  return k == nf;
}
static inline i64 days_from_civil(i64 y, i64 m, i64 d) {
  y -= m <= 2;
  i64 era = (y >= 0 ? y : y - 399) / 400;
  i64 yoe = y - era * 400;
  i64 doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  i64 doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + doe - 719468;
}
static inline bool isd(char c) { return c >= '0' && c <= '9'; }
static inline int d2(const char* p) { return (p[0] - '0') * 10 + (p[1] - '0'); }
static const i64 TS_HUGE = INT64_MAX;
// F1/F2/F3 to epoch ms. False when bad. Huge F3 values give TS_HUGE.
static bool parse_ts(const char* p, u32 n, i64& out) {
  if (n == 0) return false;
  if (isd(p[0])) {
    bool all = true;
    for (u32 i = 0; i < n; i++)
      if (!isd(p[i])) {
        all = false;
        break;
      }
    if (all) {
      u32 i = 0;
      while (i < n && p[i] == '0') i++;
      u32 rem = n - i;
      if (rem > 19) {
        out = TS_HUGE;
        return true;
      }
      u64 v = 0;
      for (; i < n; i++) v = v * 10 + (u64)(p[i] - '0');
      if (rem == 19 && v > (u64)INT64_MAX) {
        out = TS_HUGE;
        return true;
      }
      out = (i64)v;
      return true;
    }
  }
  if (n != 20 && n != 25) return false;
  static const int dp[] = {0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18};
  for (int i : dp)
    if (!isd(p[i])) return false;
  if (p[4] != '-' || p[7] != '-' || p[10] != 'T' || p[13] != ':' || p[16] != ':') return false;
  i64 off = 0;
  if (n == 20) {
    if (p[19] != 'Z') return false;
  } else {
    char sg = p[19];
    if (sg != '+' && sg != '-') return false;
    if (!isd(p[20]) || !isd(p[21]) || p[22] != ':' || !isd(p[23]) || !isd(p[24])) return false;
    int oh = d2(p + 20), om = d2(p + 23);
    if (oh > 23 || om > 59) return false;
    off = (i64)(oh * 60 + om) * 60000;
    if (sg == '-') off = -off;
  }
  int y = (p[0] - '0') * 1000 + (p[1] - '0') * 100 + (p[2] - '0') * 10 + (p[3] - '0');
  int mo = d2(p + 5), d = d2(p + 8), h = d2(p + 11), mi = d2(p + 14), s = d2(p + 17);
  if (mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || s > 59) return false;
  static const int dim[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int md = dim[mo];
  if (mo == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) md = 29;
  if (d > md) return false;
  out = (days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + s) * 1000 - off;
  return true;
}
static inline bool parse_price(const char* p, u32 n, u32& out) {  // digits, 1..10,000,000
  if (n == 0) return false;
  for (u32 i = 0; i < n; i++)
    if (!isd(p[i])) return false;
  u32 i = 0;
  while (i < n && p[i] == '0') i++;
  if (n - i > 8) return false;
  u32 v = 0;
  for (; i < n; i++) v = v * 10 + (u32)(p[i] - '0');
  if (v < 1 || v > 10000000) return false;
  out = v;
  return true;
}

// ---------------------------------------------------------------- fast time
// The date of each UTC day the window touches, as text, for comparing with ingest_ts.
struct DayStr {
  u64 k0;   // "YYYY-MM-"
  u32 ddt;  // "DDT"
  u32 pad;
};
static DayStr* g_daystr = nullptr;
// The dates near the window, keyed by their 11 bytes "YYYY-MM-DDT". A
// timestamp on any other date goes to the generic parser.
struct DateEnt {
  u64 k0;
  u32 k1;
  int32_t base;  // seconds from WS to that date's midnight
};
static DateEnt g_dates[256];
static u64 g_dmul = 0x9E3779B97F4A7C15ull;
static AINLINE u32 date_hash(u64 w0, u32 ddt) { return (u32)(((w0 ^ ((u64)ddt << 24)) * g_dmul) >> 56); }
static void init_dates() {
  struct K {
    u64 k0;
    u32 k1;
    i64 base;
  };
  vector<K> ks;
  i64 d0 = DAY0 - 12, d1 = (WE - 1) / DAYMS + 12;
  for (i64 day = d0; day <= d1; day++) {
    // civil from days
    i64 z = day + 719468;
    i64 era = (z >= 0 ? z : z - 146096) / 146097;
    i64 doe = z - era * 146097;
    i64 yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    i64 y = yoe + era * 400;
    i64 doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    i64 mp = (5 * doy + 2) / 153;
    i64 d = doy - (153 * mp + 2) / 5 + 1;
    i64 m = mp < 10 ? mp + 3 : mp - 9;
    y += m <= 2;
    if (y < 0 || y > 9999) continue;
    char b[16];
    snprintf(b, sizeof b, "%04d-%02d-%02dT", (int)y, (int)m, (int)d);
    K k;
    k.k0 = load64(b);
    k.k1 = load32(b + 8) & 0xFFFFFF;
    k.base = day * 86400 - WS / 1000;
    ks.push_back(k);
  }
  g_daystr = (DayStr*)calloc((size_t)ND + 1, sizeof(DayStr));
  for (auto& k : ks) {
    i64 d = (k.base + WS / 1000) / 86400 - DAY0;  // k.base counts from WS; back to a day number
    if (d >= 0 && d < ND) g_daystr[d].k0 = k.k0, g_daystr[d].ddt = k.k1;
  }
  u64 seed = 0x9E3779B97F4A7C15ull;
  for (int attempt = 0; attempt < 100000; attempt++) {
    memset(g_dates, 0, sizeof g_dates);
    g_dmul = seed | 1;
    bool ok = true;
    for (auto& k : ks) {
      DateEnt& e = g_dates[date_hash(k.k0, k.k1)];
      if (e.k0) {
        ok = false;
        break;
      }
      e.k0 = k.k0, e.k1 = k.k1, e.base = (int32_t)k.base;
    }
    if (ok) return;
    seed = mix(seed + attempt);
  }
  memset(g_dates, 0, sizeof g_dates);  // no perfect hash: every timestamp takes the generic path
}
// "YYYY-MM-DDTHH:MM:SS" at p (19 bytes checked) to seconds from WS.
static AINLINE bool f1_core(const char* p, i64& rel_sec) {
  u64 w0 = load64(p);
  u32 ddt = load32(p + 8) & 0xFFFFFF;
  const DateEnt& e = g_dates[date_hash(w0, ddt)];
  u64 w = load64(p + 11) ^ 0x30303A30303A3030ull;  // "HH:MM:SS"
  u64 bad = ((w + 0x060A0F060A0F060Dull) | w) & 0xF0F0F0F0F0F0F0F0ull;
  u64 t = w * 2561;
  u32 H = (u32)(t >> 8) & 0xFF, Mi = (u32)(t >> 32) & 0xFF, S = (u32)(t >> 56);
  if (UNLIKELY(((e.k0 ^ w0) | (u64)(e.k1 ^ ddt) | bad) != 0 || H > 23)) return false;
  rel_sec = e.base + (i64)(H * 3600 + Mi * 60 + S);
  return true;
}
// 25 bytes "YYYY-MM-DDTHH:MM:SS+HH:MM".
static AINLINE bool f2_core(const char* p, i64& rel_sec) {
  i64 rs;
  if (!f1_core(p, rs)) return false;
  u64 w = load64(p + 17);  // S S sign H H : M M
  u32 sign = (u32)(w >> 16) & 0xFF;
  u64 x = (w >> 24) ^ 0x30303A3030ull;
  u64 bad = ((x + 0x060A0F060Dull) | x) & 0xF0F0F0F0F0ull;
  u64 t = x * 2561;
  u32 oh = (u32)(t >> 8) & 0xFF, om = (u32)(t >> 32) & 0xFF;
  if (UNLIKELY((bad != 0) | (oh > 23) | ((sign != '+') & (sign != '-')))) return false;
  i64 off = (i64)(oh * 60 + om) * 60;
  rel_sec = sign == '-' ? rs + off : rs - off;
  return true;
}
// x: "HH:MM:SS" already XORed to digit values. Returns HHMMSS, four bits a digit.
static AINLINE u32 bcd6(u64 x) {
#if defined(__BMI2__)
  return (u32)_pext_u64(__builtin_bswap64(x), 0x0F0F000F0F000F0Full);
#else
  return (u32)(((x & 0xF) << 20) | (((x >> 8) & 0xF) << 16) | (((x >> 24) & 0xF) << 12) | (((x >> 32) & 0xF) << 8) | (((x >> 48) & 0xF) << 4) | ((x >> 56) & 0xF));
#endif
}
// The key for an ingest time given in epoch ms. False when it cannot be held exactly.
static bool ingest_key(i64 ms, u32& key) {
  if (ms < 0 || ms == TS_HUGE || ms % 1000 != 0) return false;
  i64 day = ms / DAYMS - DAY0 + INGEST_DAY_BIAS;
  if (day < 0 || day > 127) return false;
  u32 sod = (u32)((ms % DAYMS) / 1000);
  u32 h = sod / 3600, m = sod / 60 % 60, sc = sod % 60;
  key = (u32)day << 24 | (h / 10) << 20 | (h % 10) << 16 | (m / 10) << 12 | (m % 10) << 8 | (sc / 10) << 4 | (sc % 10);
  return true;
}
static AINLINE u32 p8(u64 v) {  // 8 digit values, byte 0 most significant
  v = (v * 10) + (v >> 8);
  v = (((v & 0x000000FF000000FFull) * 0x000F424000000064ull) + (((v >> 16) & 0x000000FF000000FFull) * 0x0000271000000001ull)) >> 32;
  return (u32)v;
}
static AINLINE bool f3_13(const char* p, u64& v) {  // exactly 13 digits
  u64 a = load64(p) ^ 0x3030303030303030ull;
  u64 b = ((load64(p + 5) >> 24) ^ 0x3030303030ull) << 24;
  u64 bad = (((a + 0x0606060606060606ull) | a) | ((b + 0x0606060606060606ull) | b)) & 0xF0F0F0F0F0F0F0F0ull;
  if (UNLIKELY(bad != 0)) return false;
  v = (u64)p8(a) * 100000 + p8(b);
  return true;
}

// ---------------------------------------------------------------- lookups
// IDs of at most 8 non-NUL bytes in an open-addressing table, the rest in a map.
struct ShortMap {
  struct S {
    u64 key;
    u32 val;
    u32 pad;
  };
  S* s = nullptr;
  u64 mask = 0;
  int shift = 58;
  std::unordered_map<string, u32> longm;
  void init(size_t n) {
    size_t cap = 64;
    while (cap < n * 2 + 16) cap <<= 1, shift--;
    s = (S*)calloc(cap, sizeof(S));
    mask = cap - 1;
  }
  bool insert(const char* p, u32 n, u32 val) {  // false when already there
    if (n >= 1 && n <= 8) {
      u64 k = 0;
      memcpy(&k, p, n);
      if (!has_zero_in(k, n)) {
        u64 i = hash64(k) >> shift;
        while (s[i].key) {
          if (s[i].key == k) return false;
          i = (i + 1) & mask;
        }
        s[i].key = k, s[i].val = val;
        return true;
      }
    }
    return longm.emplace(string(p, n), val).second;
  }
  // p must be readable for 8 bytes.
  AINLINE bool find(const char* p, u32 n, u32& val) const {
    if (LIKELY(n - 1 < 8u)) {
      u64 k = load64(p) & LENMASK[n];
      if (LIKELY(!has_zero_in(k, n))) {
        u64 i = hash64(k) >> shift;
        for (;;) {
          const S& e = s[i];
          if (e.key == k) {
            val = e.val;
            return true;
          }
          if (!e.key) return false;
          i = (i + 1) & mask;
        }
      }
    }
    return find_long(p, n, val);
  }
  NOINLINE bool find_long(const char* p, u32 n, u32& val) const {
    if (longm.empty()) return false;
    auto it = longm.find(string(p, n));
    if (it == longm.end()) return false;
    val = it->second;
    return true;
  }
};
// IDs that are a fixed prefix and a decimal number with no leading zero (the
// shape of every product and campaign the generator writes) are also kept in
// an array indexed by the number, which is exact for that shape and much
// cheaper than hashing. Any other ID uses the ShortMap.
struct NumMap {
  static constexpr u32 LIMIT = 1u << 20;
  u32* a = nullptr;  // value + 1, 0 when absent
  u64 pfx = 0;       // the prefix bytes
  u32 plen = 0;
  void init(const char* prefix) {
    plen = (u32)strlen(prefix);
    memcpy(&pfx, prefix, plen);
    a = (u32*)calloc(LIMIT, sizeof(u32));
  }
  // The number when the ID has the shape and is below LIMIT. p must be readable for 16 bytes.
  AINLINE bool number(const char* p, u32 n, u32& num) const {
    const u32 k = n - plen;  // digits
    if (UNLIKELY(k - 1 > 6u || ((load64(p) ^ pfx) & LENMASK[plen]) != 0)) return false;
    const u64 x = (load64(p + plen) ^ 0x3030303030303030ull) & LENMASK[k];
    if (UNLIKELY((((x + 0x0606060606060606ull) | x) & 0xF0F0F0F0F0F0F0F0ull) != 0)) return false;
    if (UNLIKELY((x & 0xFF) == 0 && k != 1)) return false;  // a leading zero: another ID
    u64 v = x << (8 * (8 - k));  // digits end at byte 7, zeros before them
    v = (v * 10) + (v >> 8);
    v = (((v & 0x000000FF000000FFull) * 0x000F424000000064ull) + (((v >> 16) & 0x000000FF000000FFull) * 0x0000271000000001ull)) >> 32;
    num = (u32)v;
    return num < LIMIT;
  }
  // Adds an ID of the shape: 1 when added, 2 when it was already there, 0
  // when the ID is not one this table holds. p need not be padded.
  int put(const char* p, u32 n, u32 val) {
    char b[24] = {0};
    if (n > 16) return 0;
    memcpy(b, p, n);
    u32 num;
    if (!number(b, n, num)) return 0;
    if (a[num]) return 2;
    a[num] = val + 1;
    return 1;
  }
};
static ShortMap g_prod, g_camp;
static NumMap g_prodn, g_campn;
// An ID is in exactly one of the two: the NumMap when it has the shape, the
// ShortMap otherwise. Adds it; false when it was already there.
static bool add_id(NumMap& nm, ShortMap& sm, const char* p, u32 n, u32 val) {
  const int r = nm.put(p, n, val);
  return r ? r == 1 : sm.insert(p, n, val);
}
// Looks an ID up. p must be readable for 16 bytes.
static AINLINE bool find_id(const NumMap& nm, const ShortMap& sm, const char* p, u32 n, u32& val) {
  u32 num;
  if (LIKELY(nm.number(p, n, num))) {
    const u32 v = nm.a[num];
    if (v) val = v - 1;  // val is left alone when the ID is unknown, as ShortMap::find does
    return v != 0;
  }
  return sm.find(p, n, val);
}
// The same for the generic parsers: any length, p need not be padded.
static NOINLINE bool find_any(const NumMap& nm, const ShortMap& sm, const char* p, u32 n, u32& val) {
  if (n > 16) return sm.find_long(p, n, val);
  char b[24] = {0};
  memcpy(b, p, n);
  return find_id(nm, sm, b, n, val);
}
static vector<string> g_cat_names;  // sorted byte-wise; the index is the rank
static u32 NCAT, UNKNOWN_CAT;
static vector<string> g_camp_id, g_camp_chan;
static u32 NCAMP, ORGANIC;

// user ID -> person name, for every ID in a link. Anything else names itself.
struct IdSlot {
  u64 key, val;
};
static IdSlot* g_ids = nullptr;
static u64 g_idcap = 0;
static AINLINE u64 id_index(u64 key) { return mulhi(hash64(key), g_idcap); }

// ---------------------------------------------------------------- memory
static bool g_hugepages = true;
// Large regions, so they can be handed back to the kernel from all threads at
// the end: process exit would otherwise tear them down on one core.
static std::mutex g_regions_mu;
static vector<std::pair<char*, size_t>> g_regions;
static void add_region(void* p, size_t n) {
  std::lock_guard<std::mutex> lk(g_regions_mu);
  g_regions.emplace_back((char*)p, n);
}
static void* big_alloc(size_t n) {  // zeroed, 2 MB aligned
  const size_t A = 2u << 20;
  char* m = (char*)mmap(nullptr, n + A, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON | MAP_NORESERVE, -1, 0);
  if (m == MAP_FAILED) {
    perror("mmap");
    exit(1);
  }
  char* a = (char*)(((uintptr_t)m + A - 1) & ~(uintptr_t)(A - 1));
#ifdef MADV_HUGEPAGE
  if (g_hugepages) madvise(a, n, MADV_HUGEPAGE);
#endif
  add_region(a, n);
  return a;
}
// The small tables a thread reads and writes at random, once per event (its
// scatter buffers, dedup and index tables, sort arrays, campaign counters),
// come from 2 MB pages as well: on 4 KB pages nearly every such access misses
// the first-level TLB. One arena per thread; nothing is given back.
struct HotArena {
  char* cur = nullptr;
  char* end = nullptr;
  void* get(size_t n) {  // zeroed, 64-byte aligned
    n = (n + 63) & ~(size_t)63;
    if ((size_t)(end - cur) < n) {
      const size_t two = 2u << 20;
      const size_t sz = std::max<size_t>(2 * two, (n + two - 1) & ~(two - 1));
      cur = (char*)big_alloc(sz);
      end = cur + sz;
    }
    void* p = cur;
    cur += n;
    return p;
  }
};
static thread_local HotArena* t_hot = nullptr;  // the running thread's, during the phases
static void* hot_alloc(size_t n) {              // zeroed, 64-byte aligned
  if (t_hot) return t_hot->get(n);
  n = (std::max<size_t>(n, 1) + 63) & ~(size_t)63;
  void* p = aligned_alloc(64, n);
  memset(p, 0, n);
  return p;
}
static const size_t BLK = 8192, BLKH = 64;
struct Blk {
  Blk* next;
  u32 used;
};
struct BlkArena {
  static constexpr size_t CHUNK = 16u << 20;
  char* cur = nullptr;
  char* end = nullptr;
  char* first = nullptr;  // the first chunk
  Blk* get() {
    if (cur == end) {
      cur = (char*)big_alloc(CHUNK);
      end = cur + CHUNK;
      if (!first) first = cur;
    }
    Blk* b = (Blk*)cur;
    cur += BLK;
    return b;
  }
  // n bytes of the first chunk that blocks have used, so are paged in; null
  // when there are not that many. For the end of the run, when no block is live.
  char* paged_in(size_t n) const {
    if (!first) return nullptr;
    const size_t have = end == first + CHUNK ? (size_t)(cur - first) : CHUNK;
    return have >= n ? first : nullptr;
  }
};
static AINLINE void nt_copy(char* dst, const char* src, size_t n) {
#if defined(__AVX2__)
  for (size_t i = 0; i < n; i += 32) _mm256_stream_si256((__m256i*)(dst + i), _mm256_load_si256((const __m256i*)(src + i)));
#else
  memcpy(dst, src, n);
#endif
}
// Appends fixed-size records to P partitions through small write-combining
// buffers, so the P output streams cost one cache/TLB miss per NREC records.
template <int RS, int NREC>
struct Scatter {
  static constexpr u32 WCB = RS * NREC;
  u32 P = 0;
  char* wc = nullptr;    // P buffers of WCB bytes
  u8* fill = nullptr;    // bytes in use in each buffer; one byte each keeps the whole array in L1
  char** cur = nullptr;  // where each partition's next flush goes, inside its block
  Blk** head = nullptr;
  Blk** tail = nullptr;
  u64* nbytes = nullptr;
  BlkArena* arena = nullptr;
  Blk** freelist = nullptr;
  void init(u32 P_, BlkArena* a, Blk** fl) {
    P = P_, arena = a, freelist = fl;
    wc = (char*)hot_alloc((size_t)P * WCB);
    static_assert(WCB <= 255, "fill is a byte");
    fill = (u8*)hot_alloc(P);
    cur = (char**)hot_alloc(P * sizeof(char*));
    head = (Blk**)hot_alloc(P * sizeof(Blk*));
    tail = (Blk**)hot_alloc(P * sizeof(Blk*));
    nbytes = (u64*)hot_alloc(P * sizeof(u64));
  }
  AINLINE void put(u32 p, const void* rec) {
    u32 f = fill[p];
    memcpy(wc + (size_t)p * WCB + f, rec, RS);
    f += RS;
    if (UNLIKELY(f == WCB)) {
      flush(p, WCB);
      f = 0;
    }
    fill[p] = (u8)f;
  }
  NOINLINE void flush(u32 p, u32 n) {
    char* c = cur[p];
    uintptr_t off = (uintptr_t)c & (BLK - 1);
    if (off == 0 || off + n > BLK) {
      Blk* b;
      if (*freelist) {
        b = *freelist;
        *freelist = b->next;
      } else {
        b = arena->get();
      }
      b->next = nullptr;
      b->used = 0;
      if (tail[p]) {
        tail[p]->used = (u32)(c - ((char*)tail[p] + BLKH));
        tail[p]->next = b;
      } else {
        head[p] = b;
      }
      tail[p] = b;
      c = (char*)b + BLKH;
    }
    if (n == WCB)
      nt_copy(c, wc + (size_t)p * WCB, WCB);
    else
      memcpy(c, wc + (size_t)p * WCB, n);
    cur[p] = c + n;
    nbytes[p] += n;
  }
  void finish() {
    for (u32 p = 0; p < P; p++) {
      if (fill[p]) flush(p, fill[p]);
      fill[p] = 0;
      if (tail[p]) tail[p]->used = (u32)(cur[p] - ((char*)tail[p] + BLKH));
    }
#if defined(__x86_64__)
    _mm_sfence();
#endif
  }
};
// Scratch arrays and output buffers. Anything large comes from big_alloc, so
// it is backed by huge pages and released in parallel at the end.
static const size_t BIG_MIN = 1u << 20;
static void* buf_alloc(size_t bytes, size_t& got) {
  if (bytes >= BIG_MIN) {
    got = (bytes + (2u << 20) - 1) & ~(size_t)((2u << 20) - 1);
    return big_alloc(got);
  }
  got = bytes;
  return hot_alloc(bytes);
}
static void buf_free(void* p, size_t bytes) {
  if (!p) return;
  if (bytes >= BIG_MIN) {
#ifdef __linux__
    madvise(p, bytes, MADV_DONTNEED);  // stays registered; a second release at the end is harmless
#endif
  }
  // a small one stays in its arena: they grow a few times and then keep their size
}
template <class T>
struct Arr {
  T* p = nullptr;
  size_t cap = 0, bytes = 0;
  void ensure(size_t n) {  // contents are not kept
    if (n > cap) {
      buf_free(p, bytes);
      p = (T*)buf_alloc((n + n / 2 + 64) * sizeof(T), bytes);
      cap = bytes / sizeof(T);
    }
  }
  void keep(size_t n) {  // contents are kept
    if (n > cap) {
      size_t nb;
      T* q = (T*)buf_alloc((n * 2 + 64) * sizeof(T), nb);
      if (p) memcpy(q, p, cap * sizeof(T));
      buf_free(p, bytes);
      p = q, bytes = nb, cap = nb / sizeof(T);
    }
  }
  T& operator[](size_t i) { return p[i]; }
};
struct Buf {
  char* p = nullptr;
  size_t n = 0, cap = 0;
  bool lent = false;  // p is memory someone else owns
  AINLINE void need(size_t k) {
    if (UNLIKELY(n + k > cap)) grow(k);
  }
  NOINLINE void grow(size_t k) {
    size_t nc;
    char* q = (char*)buf_alloc(std::max<size_t>(cap * 2, std::max<size_t>(n + k, 4u << 20)), nc);
    if (p) memcpy(q, p, n);
    if (!lent) buf_free(p, cap);
    p = q, cap = nc, lent = false;
  }
  template <class T>
  AINLINE void put(T v) {
    memcpy(p + n, &v, sizeof(T));
    n += sizeof(T);
  }
};

// ---------------------------------------------------------------- records
struct Ev {  // parsed event, before dedup
  u64 eid, uid;
  u32 ts;      // ms from WS, or TS_INVALID when the copy fails validation
  u32 ingest;  // ordered like the ingest time: day << 24 | HHMMSS in BCD
  u32 pc;      // price << 8 | category for a purchase, else 0
  u32 pad;
};
struct Ev2 {  // surviving event
  u64 uid;
  u64 tspc;  // ts << 32 | pc
};
struct Ck {  // valid click
  u64 uid, cid;
  u32 ts;  // ms from WS - 7 days
  u32 camp;
};
struct CkItem {
  u64 cid;
  u32 ts, camp;
};
static_assert(sizeof(Ev) == 32 && sizeof(Ev2) == 16 && sizeof(Ck) == 24, "record layout");

struct LSlot {  // person -> index within a partition
  u64 key;
  u32 val, pad;
};
struct Pu {  // a purchase: (person index << 32 | ts), price << 8 | category
  u64 key;
  u32 pc, pad;
};
struct Idx {
  u32* st = nullptr;    // where each field's content starts (after an opening quote)
  u32* nl = nullptr;    // newline positions
  u32* bad = nullptr;   // positions of quoting errors, ascending, 0xFFFFFFFF after the last
  u32 nnl = 0;
  bool allbad = false;  // a NUL byte in the batch: every line takes the generic path
  void init(u32 batch) {
    size_t cap = (size_t)batch + 1024;
    st = (u32*)malloc(cap * 4), nl = (u32*)malloc(cap * 4), bad = (u32*)malloc(cap * 4);
  }
};
// An append-only byte stream kept in recycled blocks: memory that is already
// paged in, and never copied to grow. An item is always written whole inside
// one piece; an item larger than a block gets a piece of its own.
struct Chain {
  char* w = nullptr;   // the next byte
  char* e = nullptr;   // end of the piece being filled
  char* b = nullptr;   // its start
  vector<iovec> segs;  // the pieces before it, in order
  BlkArena* arena = nullptr;
  Blk** freelist = nullptr;
  void init(BlkArena* a, Blk** fl) { arena = a, freelist = fl; }
  AINLINE void need(size_t k) {
    if (UNLIKELY((size_t)(e - w) < k)) next(k);
  }
  NOINLINE void next(size_t k) {
    if (w != b) segs.push_back(iovec{b, (size_t)(w - b)});
    if (k > BLK) {
      b = (char*)malloc(k);
      e = b + k;
    } else {
      Blk* x;
      if (*freelist)
        x = *freelist, *freelist = x->next;
      else
        x = arena->get();
      b = (char*)x;  // the whole block: its header is not needed again
      e = b + BLK;
    }
    w = b;
  }
  template <class T>
  AINLINE void put(T v) {
    memcpy(w, &v, sizeof(T));
    w += sizeof(T);
  }
  size_t finish() {  // closes the last piece; returns the total size
    if (w != b) segs.push_back(iovec{b, (size_t)(w - b)});
    w = e = b = nullptr;
    size_t n = 0;
    for (auto& sg : segs) n += sg.iov_len;
    return n;
  }
};
struct Out {  // user_daily columns, Parquet PLAIN
  Chain uid, day, ses, evs, rev, cat, defs;
  u64 rows = 0;
  void init(BlkArena* a, Blk** fl) {
    for (Chain* c : {&uid, &day, &ses, &evs, &rev, &cat, &defs}) c->init(a, fl);
  }
};
struct alignas(64) TC {
  int id = 0;
  HotArena hot;
  BlkArena arena;
  Blk* freelist = nullptr;
  Scatter<32, 4> evs;
  Scatter<16, 8> pes;
  Scatter<24, 8> cks;
  Scatter<4, 32> fwd;  // 32-bit words: the events of linked IDs, on their way to the person's partition
  Idx ix;
  vector<std::pair<u64, u64>> pairs;
  u64 n_lines = 0, n_slow = 0;
  double t_ev = 0, t_ck = 0;
  // phase 2
  u64* dtab = nullptr;
  size_t dcap = 0;
  vector<std::pair<Ev*, u32>> segs;
  // phase 3
  LSlot* lt = nullptr;
  u32 ltcap = 0, ltbits = 0;
  Arr<u64> pkey, eva, evb, ckey;
  Arr<u32> cnt, pos, ccnt, cpos, cidx;
  Arr<u8> plink;
  Arr<Pu> pu;
  Arr<CkItem> ck, craw;
  u32 hb[8192];
  u32 *wcnt = nullptr, *wpos = nullptr, *wact = nullptr;
  u32 nact = 0;
  u32* stamp = nullptr;  // per campaign: the last session that counted it as an assist
  u32 sid = 0;
  i64* catrev = nullptr;
  // campaign_daily, indexed [campaign * ND + day]: a person's campaigns are
  // few and their days are neighbours, so the cells they touch share cache lines
  struct Agg {
    u32 sess, asst;
  };
  struct Agg8 {
    u8 sess, asst;
  };
  // Sessions and assists are counted in one byte per cell, a table small
  // enough to stay in cache; a byte that wraps carries 256 into the wide one.
  Agg8* agg8 = nullptr;
  Agg* agg = nullptr;
  AINLINE void add_sess(size_t at) {
    if (UNLIKELY(++agg8[at].sess == 0)) agg[at].sess += 256;
  }
  AINLINE void add_asst(size_t at) {
    if (UNLIKELY(++agg8[at].asst == 0)) agg[at].asst += 256;
  }
  u32* a_purch = nullptr;
  i64* a_rev = nullptr;
  Out out;
};
static vector<TC*> g_tc;
static int g_p1bits = 4, g_p2bits = 4;
static u32 BATCH = 1u << 16;

// ---------------------------------------------------------------- stage 1
// One SIMD pass over a batch of text finds every field. A field is the bytes
// between two separators (comma or newline); when it is wrapped in quotes the
// index points past the opening quote, so stage 2 rarely looks at a quote.
// Lines whose quotes are not exactly "around a whole field" are listed in `bad`.
//
// Per 64-byte block, with S = separators and Q = quotes as bit masks:
//   P   the byte after a separator: a field's first byte
//   OQ  quotes that open a field, Qn the other quotes
//   u,t the byte after an opening / other quote
// A quote that does not open a field must be followed by a separator (t & ~S),
// an opening quote must not be (u & S), and the separator that ends a field
// has a quote before it exactly when the field opened with one. The last test
// carries each opening quote to its field's separator with one addition.
#define FIELD_LOGIC(C, N, Q)                                     \
  const u64 S = (C) | (N);                                       \
  const u64 P = (S << 1) | prevS;                                \
  const u64 OQ = (Q) & P;                                        \
  const u64 Qn = (Q) & ~P;                                       \
  const u64 u = (OQ << 1) | cOQ;                                 \
  const u64 t = (Qn << 1) | cQn;                                 \
  u64 sum;                                                       \
  const u64 k1 = __builtin_add_overflow(~S, OQ, &sum);           \
  const u64 k2 = __builtin_add_overflow(sum, carry, &sum);       \
  const u64 badm = (t & ~S) | (u & S) | ((sum ^ (t | u)) & S);   \
  const u64 Bm = (P & ~(Q)) | u;                                 \
  carry = k1 | k2, prevS = S >> 63, cOQ = OQ >> 63, cQn = Qn >> 63;
#ifndef PF_DIST
#define PF_DIST 1024
#endif
// Hides a value from the optimizer, which otherwise turns the fixed eight
// writes below into a chain of unpredictable "is it zero yet" branches.
#define OPAQUE(x) __asm__ volatile("" : "+r"(x))
static void stage1_finish(const char* T, u32 len, Idx& ix, u32* st, u32* nl, u32* bad, bool zero) {
  ix.nnl = (u32)(nl - ix.nl);
  for (int i = 0; i < 16; i++) st[i] = 0xFFFFFFFFu;
  *bad = 0xFFFFFFFFu;
  ix.allbad = zero && memchr(T, 0, len) != nullptr;  // the blocks read past len; make sure
}
// T[0] must start a line. Reads up to 63 bytes past T + len.
// The positions of a block are written by a fixed number of steps, with a
// loop only for a block that has more: seven field starts, and NLFIX
// newlines (one when lines are longer than a block, as events are).
template <int NLFIX>
static AINLINE void stage1_generic_t(const char* T, u32 len, Idx& ix) {
  const u32 nb = (len + 63) >> 6;
  u32 *st = ix.st, *nl = ix.nl, *bad = ix.bad;
  u64 prevS = 1, cOQ = 0, cQn = 0, carry = 0;
#if defined(__AVX2__)
  const __m256i VC = _mm256_set1_epi8(','), VN = _mm256_set1_epi8('\n'), VQ = _mm256_set1_epi8('"');
  __m256i zmin = _mm256_set1_epi8(-1);
#elif defined(__aarch64__)
  const uint8x16_t bm = {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80};
  uint8x16_t zmin = vdupq_n_u8(255);
#else
  bool zany = false;
#endif
  for (u32 b = 0; b < nb; b++) {
    const char* p = T + ((size_t)b << 6);
    __builtin_prefetch(p + PF_DIST);  // the hardware prefetcher restarts at every 4 KB page
    u64 C, N, Q;
#if defined(__AVX2__)
    {
      __m256i x = _mm256_loadu_si256((const __m256i*)p), y = _mm256_loadu_si256((const __m256i*)(p + 32));
      C = (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x, VC)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y, VC)) << 32);
      N = (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x, VN)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y, VN)) << 32);
      Q = (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x, VQ)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y, VQ)) << 32);
      zmin = _mm256_min_epu8(zmin, _mm256_min_epu8(x, y));
    }
#elif defined(__aarch64__)
    {
      uint8x16_t v0 = vld1q_u8((const u8*)p), v1 = vld1q_u8((const u8*)p + 16), v2 = vld1q_u8((const u8*)p + 32), v3 = vld1q_u8((const u8*)p + 48);
      auto mask = [&](u8 ch) -> u64 {
        uint8x16_t c = vdupq_n_u8(ch);
        uint8x16_t t0 = vandq_u8(vceqq_u8(v0, c), bm), t1 = vandq_u8(vceqq_u8(v1, c), bm);
        uint8x16_t t2 = vandq_u8(vceqq_u8(v2, c), bm), t3 = vandq_u8(vceqq_u8(v3, c), bm);
        uint8x16_t s0 = vpaddq_u8(t0, t1), s1 = vpaddq_u8(t2, t3);
        s0 = vpaddq_u8(s0, s1);
        s0 = vpaddq_u8(s0, s0);
        return vgetq_lane_u64(vreinterpretq_u64_u8(s0), 0);
      };
      C = mask(','), N = mask('\n'), Q = mask('"');
      zmin = vminq_u8(zmin, vminq_u8(vminq_u8(v0, v1), vminq_u8(v2, v3)));
    }
#else
    C = N = Q = 0;
    for (int i = 0; i < 64; i++) {
      char c = p[i];
      C |= (u64)(c == ',') << i, N |= (u64)(c == '\n') << i, Q |= (u64)(c == '"') << i;
      zany |= c == 0;
    }
#endif
    FIELD_LOGIC(C, N, Q)
    const u32 base = b << 6;
    {
      u32 c = (u32)std::popcount(Bm);
      u64 m = Bm;
      for (int i = 0; i < 7; i++) {
        st[i] = base + tz64(m);
        m &= m - 1;
        OPAQUE(m);
      }
      if (UNLIKELY(c > 7)) {
        for (u32 i = 7; i < c; i++) {
          st[i] = base + tz64(m);
          m &= m - 1;
        }
      }
      st += c;
    }
    {
      u32 c = (u32)std::popcount(N);
      u64 m = N;
      nl[0] = base + tz64(m);
      if (NLFIX == 2) {
        m &= m - 1;
        OPAQUE(m);
        nl[1] = base + tz64(m);
      }
      if (UNLIKELY(c > (u32)NLFIX)) {
        m &= m - 1;
        for (u32 i = NLFIX; i < c; i++) {
          nl[i] = base + tz64(m);
          m &= m - 1;
        }
      }
      nl += c;
    }
    if (UNLIKELY(badm != 0)) {
      u64 m = badm;
      while (m) {
        *bad++ = base + tz64(m);
        m &= m - 1;
      }
    }
  }
#if defined(__AVX2__)
  bool zero = _mm256_movemask_epi8(_mm256_cmpeq_epi8(zmin, _mm256_setzero_si256())) != 0;
#elif defined(__aarch64__)
  bool zero = vminvq_u8(zmin) == 0;
#else
  bool zero = zany;
#endif
  stage1_finish(T, len, ix, st, nl, bad, zero);
}
static void stage1_generic(const char* T, u32 len, Idx& ix) { stage1_generic_t<2>(T, len, ix); }
static void stage1_generic_long(const char* T, u32 len, Idx& ix) { stage1_generic_t<1>(T, len, ix); }
#if defined(__x86_64__)
// The same pass with AVX-512: mask registers straight from the compares, and
// VBMI2's byte compress to turn a mask into positions. Only for CPUs with
// VBMI2 (Ice Lake and later): on Skylake-X and Cascade Lake any 512-bit
// instruction lowers the core's frequency and costs more than it saves.
__attribute__((target("avx512f,avx512bw,avx512vl,avx512dq,avx512vbmi,avx512vbmi2,bmi,bmi2,popcnt,lzcnt")))
static void stage1_vbmi2(const char* T, u32 len, Idx& ix) {
  const u32 nb = (len + 63) >> 6;
  u32 *st = ix.st, *nl = ix.nl, *bad = ix.bad;
  u64 prevS = 1, cOQ = 0, cQn = 0, carry = 0;
  const __m512i VC = _mm512_set1_epi8(','), VN = _mm512_set1_epi8('\n'), VQ = _mm512_set1_epi8('"');
  const __m512i IOTA = _mm512_set_epi8(63, 62, 61, 60, 59, 58, 57, 56, 55, 54, 53, 52, 51, 50, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34, 33, 32,
                                       31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
  __m512i zmin = _mm512_set1_epi8(-1);
  for (u32 b = 0; b < nb; b++) {
    const char* p = T + ((size_t)b << 6);
    _mm_prefetch(p + PF_DIST, _MM_HINT_T0);
    const __m512i v = _mm512_loadu_si512((const void*)p);
    const u64 C = _mm512_cmpeq_epi8_mask(v, VC), N = _mm512_cmpeq_epi8_mask(v, VN), Q = _mm512_cmpeq_epi8_mask(v, VQ);
    zmin = _mm512_min_epu8(zmin, v);
    FIELD_LOGIC(C, N, Q)
    const u32 base = b << 6;
    const __m512i vb = _mm512_set1_epi32((int)base);
    {
      const __m512i comp = _mm512_maskz_compress_epi8(Bm, IOTA);
      _mm512_storeu_si512((void*)st, _mm512_add_epi32(_mm512_cvtepu8_epi32(_mm512_castsi512_si128(comp)), vb));
      const u32 c = (u32)_mm_popcnt_u64(Bm);
      if (UNLIKELY(c > 16)) {
        _mm512_storeu_si512((void*)(st + 16), _mm512_add_epi32(_mm512_cvtepu8_epi32(_mm512_extracti32x4_epi32(comp, 1)), vb));
        _mm512_storeu_si512((void*)(st + 32), _mm512_add_epi32(_mm512_cvtepu8_epi32(_mm512_extracti32x4_epi32(comp, 2)), vb));
        _mm512_storeu_si512((void*)(st + 48), _mm512_add_epi32(_mm512_cvtepu8_epi32(_mm512_extracti32x4_epi32(comp, 3)), vb));
      }
      st += c;
    }
    {
      const __m512i comp = _mm512_maskz_compress_epi8(N, IOTA);
      _mm512_storeu_si512((void*)nl, _mm512_add_epi32(_mm512_cvtepu8_epi32(_mm512_castsi512_si128(comp)), vb));
      const u32 c = (u32)_mm_popcnt_u64(N);
      if (UNLIKELY(c > 16)) {
        _mm512_storeu_si512((void*)(nl + 16), _mm512_add_epi32(_mm512_cvtepu8_epi32(_mm512_extracti32x4_epi32(comp, 1)), vb));
        _mm512_storeu_si512((void*)(nl + 32), _mm512_add_epi32(_mm512_cvtepu8_epi32(_mm512_extracti32x4_epi32(comp, 2)), vb));
        _mm512_storeu_si512((void*)(nl + 48), _mm512_add_epi32(_mm512_cvtepu8_epi32(_mm512_extracti32x4_epi32(comp, 3)), vb));
      }
      nl += c;
    }
    if (UNLIKELY(badm != 0)) {
      u64 m = badm;
      while (m) {
        *bad++ = base + (u32)_tzcnt_u64(m);
        m &= m - 1;
      }
    }
  }
  bool zero = _mm512_cmpeq_epi8_mask(zmin, _mm512_setzero_si512()) != 0;
  stage1_finish(T, len, ix, st, nl, bad, zero);
}
#endif
static void (*g_stage1)(const char*, u32, Idx&) = stage1_generic;
static void (*g_stage1_events)(const char*, u32, Idx&) = stage1_generic_long;  // for files of long lines
static AINLINE u32 isterm(u32 c) { return (c == ',') | (c == '"'); }  // what can follow a field's content
static AINLINE u32 quoted(const char* T, u32 a) { return (T + a)[-1] == '"'; }  // a is a field's start

// ---------------------------------------------------------------- events
static NOINLINE void slow_event(TC& tc, const char* s, const char* e) {
  tc.n_slow++;
  const char* fp[7];
  u32 fl[7];
  if (!split(s, e, 7, fp, fl)) return;
  if (fl[0] == 0) return;
  Ev r;
  r.eid = id_key(fp[0], fl[0], 'e');
  r.uid = 0, r.ts = TS_INVALID, r.pc = 0, r.pad = 0;
  i64 ts = 0;
  bool ok = fl[1] > 0 && parse_ts(fp[2], fl[2], ts) && ts >= WS && ts < WE;
  int type = 0;
  if (ok) {
    std::string_view ty(fp[4], fl[4]);
    if (ty == "view")
      type = 0;
    else if (ty == "cart")
      type = 1;
    else if (ty == "purchase")
      type = 2;
    else
      ok = false;
  }
  ok = ok && fl[5] > 0;
  u32 price = 0;
  if (ok && type == 2) ok = parse_price(fp[6], fl[6], price);
  i64 ing;
  if (!parse_ts(fp[3], fl[3], ing)) {
    ok = false;
    r.ingest = 0xFFFFFFFFu;
  } else {
    // Exact only as whole seconds within about two months of the window.
    if (!ingest_key(ing, r.ingest)) {
      g_bail.store(true, std::memory_order_relaxed);
      return;
    }
    if (ok && ing - ts > DAYMS) ok = false;
  }
  if (ok) {
    r.uid = id_key(fp[1], fl[1], 'u');
    r.ts = (u32)(ts - WS);
    if (type == 2) {
      u32 cat = UNKNOWN_CAT;
      find_any(g_prodn, g_prod, fp[5], fl[5], cat);
      r.pc = price << 8 | cat;
    }
  }
  tc.evs.put((u32)(hash64(r.eid) >> (64 - g_p1bits)), &r);
}
// A timestamp none of the fast forms took, as ms from WS; negative when bad or outside [WS, WE).
static NOINLINE i64 ts_generic(const char* p, u32 n, i64 lo, i64 hi) {
  i64 ms;
  if (!parse_ts(p, n, ms) || ms < lo || ms >= hi) return -1;
  return ms - lo;
}
// S holds the starts of the line's seven fields; the line ends at nl. Returns
// false for the few lines the generic parser has to take.
static AINLINE bool fast_event(TC& tc, const char* T, const u32* S, u32 nl, char* wc, u8* fill, int p1sh) {
  const u32 a0 = S[0], a1 = S[1], a2 = S[2], a3 = S[3], a4 = S[4], a5 = S[5];
  const u32 q0 = quoted(T, a0), q1 = quoted(T, a1), q2 = quoted(T, a2), q3 = quoted(T, a3);
  const u32 L0 = a1 - a0 - 1 - q0 - q1, L1 = a2 - a1 - 1 - q1 - q2, L2 = a3 - a2 - 1 - q2 - q3;
  if (UNLIKELY((u32)(L0 - 2) > 7u || T[a0] != 'e')) return false;
  const u64 eid = load64(T + a0 + 1) & LENMASK[L0 - 1];
  const char* ip = T + a3;
  if (UNLIKELY(ip[19] != 'Z' || !isterm((u8)ip[20]))) return false;
  const u64 iw = load64(ip + 11) ^ 0x30303A30303A3030ull;  // "HH:MM:SS"
  if (UNLIKELY((((iw + 0x060A0F060A0F060Dull) | iw) & 0xF0F0F0F0F0F0F0F0ull) != 0)) return false;
  const u32 bcd = bcd6(iw);
  if (UNLIKELY(bcd >= 0x240000u)) return false;
  bool valid = true;
  u64 uid = 0;
  if (LIKELY((u32)(L1 - 2) <= 7u && T[a1] == 'u'))
    uid = load64(T + a1 + 1) & LENMASK[L1 - 1];
  else if (L1 != 0)
    return false;  // an ID that needs interning
  else
    valid = false;
  const char* tp = T + a2;
  i64 rel, rs;
  u64 v;
  if (L2 == 20 && tp[19] == 'Z' && LIKELY(f1_core(tp, rs)))
    rel = rs * 1000;
  else if (L2 == 13 && LIKELY(f3_13(tp, v)))
    rel = (i64)v - WS;
  else if (L2 == 25 && LIKELY(f2_core(tp, rs)))
    rel = rs * 1000;
  else
    rel = ts_generic(tp, L2, WS, WE);
  valid &= (u64)rel < WLEN;
  // ingest_ts on the event's own UTC date cannot be more than 24 hours after
  // it, and gives the key directly. Any other date is worked out in full.
  u32 ikey;
  {
    const u32 day = valid ? (u32)(((u64)rel + WS_TOD) / (u64)DAYMS) : (u32)ND;
    const DayStr& ds = g_daystr[day];
    if (LIKELY(load64(ip) == ds.k0 && (load32(ip + 8) & 0xFFFFFF) == ds.ddt)) {
      ikey = ((day + (u32)INGEST_DAY_BIAS) << 24) | bcd;
    } else {
      i64 ing;
      if (UNLIKELY(!parse_ts(ip, 20, ing) || !ingest_key(ing, ikey))) return false;
      valid &= ing - (rel + WS) <= DAYMS;  // not late
    }
  }
  const u64 tw = load64(T + a4);
  const u32 t4 = (u32)tw;
  u32 pc = 0;
  if (LIKELY((t4 == 0x77656976u || t4 == 0x74726163u) && isterm((u32)(tw >> 32) & 0xFF))) {  // view, cart
    valid &= !isterm((u8)T[a5]);  // product_id not empty
  } else if (tw == 0x6573616863727570ull && isterm((u8)T[a4 + 8])) {  // purchase
    const u32 a6 = S[6];
    const u32 q5 = quoted(T, a5), q6 = quoted(T, a6);
    const u32 L5 = a6 - a5 - 1 - q5 - q6;
    u32 price = 0, cat = UNKNOWN_CAT;
    valid &= L5 != 0;
    valid &= parse_price(T + a6, nl - a6 - q6, price);
    if (valid) {
      find_id(g_prodn, g_prod, T + a5, L5, cat);
      pc = price << 8 | cat;
    }
  } else {
    valid = false;
  }
  const u32 p = (u32)(hash64(eid) >> p1sh);
  u32 f = fill[p];
  Ev* d = (Ev*)(wc + (size_t)p * 128 + f);
  d->eid = eid, d->uid = uid, d->ts = valid ? (u32)rel : TS_INVALID, d->ingest = ikey, d->pc = pc, d->pad = 0;
  f += 32;
  if (UNLIKELY(f == 128)) {
    tc.evs.flush(p, 128);
    f = 0;
  }
  fill[p] = (u8)f;
  return true;
}
// A line of NF fields is regular when its newline is the separator after the
// last of them, and it has no quoting error.
#define REGULAR(NF) LIKELY((sp[(NF) - 1] <= nl) & (sp[NF] > nl) & (*bp > nl))
#define RESYNC()                \
  do {                          \
    while (sp[1] <= nl) sp++;   \
    sp++;                       \
    while (*bp <= nl) bp++;     \
  } while (0)
// How many of the batch's lines start before maxstart.
static AINLINE u32 lines_owned(const u32* NL, u32 nnl, u32 maxstart) {
  if (LIKELY(nnl < 2 || NL[nnl - 2] + 1 < maxstart)) return nnl;
  return 1 + (u32)(std::lower_bound(NL, NL + nnl - 1, maxstart - 1) - NL);
}
static u32 stage2_events(TC& tc, const char* T, u32 nnl, u32 maxstart) {
  const u32 *sp = tc.ix.st, *NL = tc.ix.nl, *bp = tc.ix.bad;
  char* const wc = tc.evs.wc;
  u8* const fill = tc.evs.fill;
  const int p1sh = 64 - g_p1bits;
  const u32 n = lines_owned(NL, nnl, maxstart);
  u32 ls = 0;
  for (u32 i = 0; i < n; i++) {
    const u32 nl = NL[i];
    if (REGULAR(7) && LIKELY(fast_event(tc, T, sp, nl, wc, fill, p1sh))) {
      sp += 7;
    } else {
      slow_event(tc, T + ls, T + nl);
      RESYNC();
    }
    ls = nl + 1;
  }
  tc.n_lines += n;
  return ls;
}

// ---------------------------------------------------------------- clicks
struct CkTmp {
  u64 cid, uid;
  u32 ts, camp;
};
// A click goes to the partition of its user ID, as an event does.
static AINLINE void emit_click(TC& tc, u64 cid, u64 uid, u32 ts, u32 camp) {
  Ck r;
  r.uid = uid, r.cid = cid, r.ts = ts, r.camp = camp;
  tc.cks.put((u32)(hash64(uid) >> (64 - g_p2bits)), &r);
}
static NOINLINE void slow_click(TC& tc, const char* s, const char* e) {
  tc.n_slow++;
  const char* fp[4];
  u32 fl[4];
  if (!split(s, e, 4, fp, fl)) return;
  if (fl[0] == 0 || fl[1] == 0) return;
  i64 ts;
  if (!parse_ts(fp[3], fl[3], ts) || ts < WS - 7 * DAYMS || ts >= WE) return;
  u32 camp;
  if (!find_any(g_campn, g_camp, fp[2], fl[2], camp)) return;
  emit_click(tc, id_key(fp[0], fl[0], 'k'), id_key(fp[1], fl[1], 'u'), (u32)(ts - (WS - 7 * DAYMS)), camp);
}
// Returns 1 when out holds a valid click, 0 when the click is dropped, -1 for the generic parser.
static AINLINE int fast_click(const char* T, const u32* S, u32 nl, CkTmp& out) {
  const u32 a0 = S[0], a1 = S[1], a2 = S[2], a3 = S[3];
  const u32 q0 = quoted(T, a0), q1 = quoted(T, a1), q2 = quoted(T, a2), q3 = quoted(T, a3);
  const u32 L0 = a1 - a0 - 1 - q0 - q1, L1 = a2 - a1 - 1 - q1 - q2, L2 = a3 - a2 - 1 - q2 - q3, L3 = nl - a3 - q3;
  if (UNLIKELY((u32)(L0 - 2) > 7u || T[a0] != 'k' || (u32)(L1 - 2) > 7u || T[a1] != 'u')) return -1;
  out.cid = load64(T + a0 + 1) & LENMASK[L0 - 1];
  out.uid = load64(T + a1 + 1) & LENMASK[L1 - 1];
  if (UNLIKELY(!find_id(g_campn, g_camp, T + a2, L2, out.camp))) return 0;
  const char* tp = T + a3;
  i64 rel, rs;
  u64 v;
  if (L3 == 20 && tp[19] == 'Z' && LIKELY(f1_core(tp, rs)))
    rel = rs * 1000 + SEVEN_MS;
  else if (L3 == 13 && LIKELY(f3_13(tp, v)))
    rel = (i64)v - WS + SEVEN_MS;
  else if (L3 == 25 && LIKELY(f2_core(tp, rs)))
    rel = rs * 1000 + SEVEN_MS;
  else
    rel = ts_generic(tp, L3, WS - SEVEN_MS, WE);
  if (UNLIKELY((u64)rel >= CWLEN)) return 0;
  out.ts = (u32)rel;
  return 1;
}
static u32 stage2_clicks(TC& tc, const char* T, u32 nnl, u32 maxstart) {
  const u32 *sp = tc.ix.st, *NL = tc.ix.nl, *bp = tc.ix.bad;
  const u32 n = lines_owned(NL, nnl, maxstart);
  u32 ls = 0;
  CkTmp c;
  for (u32 i = 0; i < n; i++) {
    const u32 nl = NL[i];
    int r = -1;
    if (REGULAR(4) && LIKELY((r = fast_click(T, sp, nl, c)) >= 0)) {
      sp += 4;
      if (r) emit_click(tc, c.cid, c.uid, c.ts, c.camp);
    } else {
      slow_click(tc, T + ls, T + nl);
      RESYNC();
    }
    ls = nl + 1;
  }
  tc.n_lines += n;
  return ls;
}

// ---------------------------------------------------------------- links
static void link_line(TC& tc, const char* s, const char* e) {
  const char* fp[2];
  u32 fl[2];
  if (!split(s, e, 2, fp, fl)) return;
  if (fl[0] == 0 || fl[1] == 0) return;
  tc.pairs.emplace_back(id_key(fp[0], fl[0], 'u'), id_key(fp[1], fl[1], 'u'));
}
static u32 stage2_links(TC& tc, const char* T, u32 nnl, u32 maxstart) {
  const u32 *sp = tc.ix.st, *NL = tc.ix.nl, *bp = tc.ix.bad;
  const u32 n = lines_owned(NL, nnl, maxstart);
  u32 ls = 0;
  for (u32 i = 0; i < n; i++) {
    const u32 nl = NL[i];
    bool done = false;
    if (REGULAR(2)) {  // only then are sp[0] and sp[1] this line's fields
      const u32 a0 = sp[0], a1 = sp[1];
      const u32 q0 = quoted(T, a0), q1 = quoted(T, a1);
      const u32 L0 = a1 - a0 - 1 - q0 - q1, L1 = nl - a1 - q1;
      if (LIKELY((u32)(L0 - 2) <= 7u && T[a0] == 'u' && (u32)(L1 - 2) <= 7u && T[a1] == 'u')) {
        tc.pairs.emplace_back(load64(T + a0 + 1) & LENMASK[L0 - 1], load64(T + a1 + 1) & LENMASK[L1 - 1]);
        sp += 2;
        done = true;
      }
    }
    if (!done) {
      link_line(tc, T + ls, T + nl);
      RESYNC();
    }
    ls = nl + 1;
  }
  return ls;
}

// ---------------------------------------------------------------- files
struct MFile {
  const char* p = nullptr;
  size_t n = 0;
};
// Maps a file with at least one zero page after its last byte, so the parser
// may read a little past the end.
static MFile map_file(const string& path) {
  MFile f;
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) return f;
  struct stat st;
  if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size == 0) {
    close(fd);
    return f;
  }
  size_t n = (size_t)st.st_size;
  size_t pg = (size_t)sysconf(_SC_PAGESIZE);
  size_t maplen = ((n + pg - 1) & ~(pg - 1)) + pg;
  char* base = (char*)mmap(nullptr, maplen, PROT_READ, MAP_PRIVATE | MAP_ANON, -1, 0);
  if (base == MAP_FAILED) {
    perror("mmap");
    exit(1);
  }
  if (mmap(base, n, PROT_READ, MAP_PRIVATE | MAP_FIXED, fd, 0) == MAP_FAILED) {
    perror("mmap file");
    exit(1);
  }
  close(fd);
  f.p = base, f.n = n;
  return f;
}
static vector<string> list_csv(const string& dir) {
  vector<string> out;
  DIR* d = opendir(dir.c_str());
  if (!d) return out;
  while (dirent* e = readdir(d)) {
    string name = e->d_name;
    if (name.empty() || name[0] == '.') continue;
    if (name.size() < 4 || name.compare(name.size() - 4, 4, ".csv") != 0) continue;
    out.push_back(dir + "/" + name);
  }
  closedir(d);
  std::sort(out.begin(), out.end());
  return out;
}
enum Kind { K_EVENTS, K_CLICKS, K_LINKS };
static void slow_line(TC& tc, Kind kind, const char* s, const char* e) {
  if (kind == K_EVENTS)
    slow_event(tc, s, e);
  else if (kind == K_CLICKS)
    slow_click(tc, s, e);
  else
    link_line(tc, s, e);
}
static u32 stage2_slow(TC& tc, Kind kind, const char* T, u32 nnl, u32 maxstart) {
  u32 ls = 0, li = 0;
  while (li < nnl && ls < maxstart) {
    u32 nl = tc.ix.nl[li++];
    slow_line(tc, kind, T + ls, T + nl);
    ls = nl + 1;
  }
  return ls;
}
// Parses the lines of f that start in [s, e). The first line of a file is its header.
static void parse_chunk(TC& tc, const MFile& f, size_t s, size_t e, Kind kind) {
  const char* B = f.p;
  size_t n = f.n;
  const char* nlp = (const char*)memchr(B + (s ? s - 1 : 0), '\n', n - (s ? s - 1 : 0));
  if (!nlp) return;
  size_t pos = (size_t)(nlp - B) + 1;
  while (pos < e && pos < n) {
    size_t lim = std::min(n, pos + BATCH);
    (kind == K_EVENTS ? g_stage1_events : g_stage1)(B + pos, (u32)(lim - pos), tc.ix);
    u32 nnl = tc.ix.nnl;
    if (nnl == 0) {  // a line longer than a batch, or a last line with no newline
      size_t le = n;
      if (lim < n) {
        nlp = (const char*)memchr(B + lim, '\n', n - lim);
        if (nlp) le = (size_t)(nlp - B);
      }
      slow_line(tc, kind, B + pos, B + le);
      pos = le + 1;
      continue;
    }
    u32 maxstart = (u32)std::min<size_t>(e - pos, 0xFFFFFFFFu);
    u32 used;
    if (UNLIKELY(tc.ix.allbad))
      used = stage2_slow(tc, kind, B + pos, nnl, maxstart);
    else if (kind == K_EVENTS)
      used = stage2_events(tc, B + pos, nnl, maxstart);
    else if (kind == K_CLICKS)
      used = stage2_clicks(tc, B + pos, nnl, maxstart);
    else
      used = stage2_links(tc, B + pos, nnl, maxstart);
    pos += used;
  }
}

// Unmaps the pages of a parsed chunk now, on this thread, so that process exit
// does not have to tear down the whole input on one core. A neighbour that
// still reads a few of them simply maps them again.
static int g_release = 2;  // 2: everything in parallel just before exit, 1: also each chunk once parsed, 0: never
static void release_chunk(const MFile& f, size_t s, size_t e) {
#ifdef __linux__
  if (g_release != 1) return;
  const size_t pg = 4096;
  size_t a = (s + pg - 1) & ~(pg - 1), b = e & ~(pg - 1);
  if (b > a) madvise((void*)(f.p + a), b - a, MADV_DONTNEED);
#else
  (void)f, (void)s, (void)e;
#endif
}
// ---------------------------------------------------------------- links -> people
static vector<MFile> g_link_files;
// Union-find over the IDs in links, held in the lookup table itself: while
// building, a slot's val is the slot of its parent (itself for a root). The
// root of a set is always its byte-wise smallest ID, so it is the person's name.
static void do_links(TC& tc) {
  size_t bytes = 0;
  for (auto& f : g_link_files) bytes += f.n;
  tc.pairs.reserve(bytes / 12 + 16);
  for (auto& f : g_link_files) parse_chunk(tc, f, 0, f.n, K_LINKS);
  if (VERBOSE) fprintf(stderr, "[%8.1f ms] links: parsed\n", now_ms());
  auto& pairs = tc.pairs;
  const size_t np = pairs.size();
  const u64 cap = std::max<u64>(64, (u64)np * 3);
  IdSlot* ids = (IdSlot*)big_alloc(cap * sizeof(IdSlot));
  g_ids = ids, g_idcap = cap;
  auto get = [&](u64 k) -> u64 {
    u64 i = id_index(k);
    for (;;) {
      IdSlot& s = ids[i];
      if (s.key == k) return i;
      if (!s.key) {
        s.key = k, s.val = i;
        return i;
      }
      if (++i == cap) i = 0;
    }
  };
  auto find = [&](u64 x) -> u64 {
    for (;;) {
      u64 p = ids[x].val;
      if (p == x) return x;
      u64 g = ids[p].val;
      ids[x].val = g;  // path halving
      x = g;
    }
  };
  const size_t PB = 16;
  u64 sa[PB], sb[PB];
  for (size_t k = 0; k < std::min(np, PB); k++) {
    __builtin_prefetch(&ids[id_index(pairs[k].first)]);
    __builtin_prefetch(&ids[id_index(pairs[k].second)]);
  }
  for (size_t i = 0; i < np; i += PB) {
    size_t j = std::min(np, i + PB), j2 = std::min(np, j + PB);
    for (size_t k = j; k < j2; k++) {  // the next batch's home slots
      __builtin_prefetch(&ids[id_index(pairs[k].first)]);
      __builtin_prefetch(&ids[id_index(pairs[k].second)]);
    }
    for (size_t k = i; k < j; k++) {  // this batch's slots, then their parents
      u64 x = get(pairs[k].first), y = get(pairs[k].second);
      sa[k - i] = x, sb[k - i] = y;
      __builtin_prefetch(&ids[ids[x].val]);
      __builtin_prefetch(&ids[ids[y].val]);
    }
    for (size_t k = i; k < j; k++) {
      u64 x = find(sa[k - i]), y = find(sb[k - i]);
      if (x == y) continue;
      if (id_less(ids[x].key, ids[y].key, 'u'))
        ids[y].val = x;
      else
        ids[x].val = y;
    }
  }
  if (VERBOSE) fprintf(stderr, "[%8.1f ms] links: joined\n", now_ms());
  // Replace every ID's parent slot by its root's key, in one sweep. Behind the
  // sweep a val is already the root's key and ahead of it still a parent slot,
  // so a chain is followed only until it reaches its root or a finished slot.
  const u64 PD = 16;
  for (u64 i = 0; i < cap; i++) {
    if (i + PD < cap) __builtin_prefetch(&ids[ids[i + PD].val]);
    if (!ids[i].key) continue;
    u64 x = i, name;
    for (;;) {
      const u64 par = ids[x].val;
      if (par == x) {
        name = ids[x].key;
        break;
      }
      if (par < i) {
        name = ids[par].val;
        break;
      }
      x = par;
    }
    ids[i].val = name;
  }
}

// ---------------------------------------------------------------- phase 2
static void phase2_partition(TC& tc, u32 p) {
  tc.segs.clear();
  size_t bytes = 0;
  for (int t = 0; t < NT; t++)
    for (Blk* b = g_tc[t]->evs.head[p]; b; b = b->next) {
      tc.segs.emplace_back((Ev*)((char*)b + BLKH), b->used / (u32)sizeof(Ev));
      bytes += b->used;
    }
  const size_t N = bytes / sizeof(Ev);
  if (!N) return;
  size_t cap = 64;
  int sb = 6;
  while (cap * 2 < 3 * N) cap <<= 1, sb++;  // at most two thirds full
  if (cap > tc.dcap) {
    tc.dtab = (u64*)hot_alloc(cap * 8);
    tc.dcap = cap;
  }
  const u64 mask = cap - 1;
  const int shift = 64 - g_p1bits - sb;
  const size_t nseg = tc.segs.size();
  // Pass 1: find every event_id's copies; all but the one ingested first lose their ts.
  static_assert((BLK - BLKH) / sizeof(Ev) <= 256, "a record's index in its block fits a byte");
  if (LIKELY(nseg <= 255)) {
    // Slots of four bytes, so that the table stays in cache: 16 bits of the
    // hash, then the record as (its block + 1, its index in the block).
    u32* const tab = (u32*)tc.dtab;
    memset(tab, 0, cap * 4);
    const auto* const segs = tc.segs.data();
    for (size_t si = 0; si < nseg; si++) {
      Ev* const base = segs[si].first;
      const u32 n = segs[si].second;
      const u32 where = (u32)(si + 1) << 8;
      for (u32 j = 0; j < n; j++) {
        Ev* const r = base + j;
        const u64 h = hash64(r->eid);
        u64 i = (h >> shift) & mask;
        const u32 tag = (u32)(h >> (shift - 16)) & 0xFFFF;
        const u32 enc = (tag << 16) | where | j;
        for (;;) {
          const u32 s = tab[i];
          if (LIKELY(!s)) {
            tab[i] = enc;
            break;
          }
          if (UNLIKELY((s >> 16) == tag)) {
            Ev* q = segs[((s >> 8) & 0xFF) - 1].first + (s & 0xFF);
            if (q->eid == r->eid) {
              if (r->ingest < q->ingest) {
                q->ts = TS_INVALID;
                tab[i] = enc;
              } else {
                r->ts = TS_INVALID;
              }
              break;
            }
          }
          i = (i + 1) & mask;
        }
      }
    }
  } else {
    u64* const tab = tc.dtab;
    memset(tab, 0, cap * 8);
    const u64 PTR = (1ull << 48) - 1;
    for (auto& sg : tc.segs) {
      Ev* r = sg.first;
      Ev* const end = r + sg.second;
      for (; r < end; r++) {
        const u64 h = hash64(r->eid);
        u64 i = (h >> shift) & mask;
        const u64 tag = (h >> (shift - 16)) & 0xFFFF;
        const u64 enc = (u64)(uintptr_t)r | (tag << 48);
        for (;;) {
          const u64 s = tab[i];
          if (LIKELY(!s)) {
            tab[i] = enc;
            break;
          }
          if (UNLIKELY((s >> 48) == tag)) {
            Ev* q = (Ev*)(uintptr_t)(s & PTR);
            if (q->eid == r->eid) {
              if (r->ingest < q->ingest) {
                q->ts = TS_INVALID;
                tab[i] = enc;
              } else {
                r->ts = TS_INVALID;
              }
              break;
            }
          }
          i = (i + 1) & mask;
        }
      }
    }
  }
  // Pass 2: the survivors, by user ID. Who each ID belongs to is looked up
  // later, once per ID instead of once per event.
  const int sh2 = 64 - g_p2bits;
  char* const wc = tc.pes.wc;
  u8* const fill = tc.pes.fill;
  for (auto& sg : tc.segs) {
    const Ev* const r = sg.first;
    const u32 n = sg.second;
    for (u32 j = 0; j < n; j++) {
      const u32 ts = r[j].ts;
      if (UNLIKELY(ts == TS_INVALID)) continue;
      const u64 uid = r[j].uid;
      const u32 q = (u32)(hash64(uid) >> sh2);
      u32 f = fill[q];
      Ev2* d = (Ev2*)(wc + (size_t)q * 128 + f);
      d->uid = uid, d->tspc = ((u64)ts << 32) | r[j].pc;
      f += 16;
      if (UNLIKELY(f == 128)) {
        tc.pes.flush(q, 128);
        f = 0;
      }
      fill[q] = (u8)f;
    }
  }
  for (int t = 0; t < NT; t++) {  // hand the blocks to this thread's scatter
    Blk* b = g_tc[t]->evs.head[p];
    while (b) {
      Blk* nx = b->next;
      b->next = tc.freelist;
      tc.freelist = b;
      b = nx;
    }
  }
}

// ---------------------------------------------------------------- phase 3
static void radix_hi32(u64* a, u64* tmp, u32 n) {  // stable, by the high 32 bits
  static thread_local u32 c0[2048], c1[2048], c2[1024];
  memset(c0, 0, sizeof c0), memset(c1, 0, sizeof c1), memset(c2, 0, sizeof c2);
  for (u32 i = 0; i < n; i++) {
    u32 v = (u32)(a[i] >> 32);
    c0[v & 2047]++, c1[(v >> 11) & 2047]++, c2[v >> 22]++;
  }
  u32 s0 = 0, s1 = 0, s2 = 0;
  for (int i = 0; i < 2048; i++) {
    u32 t = c0[i];
    c0[i] = s0, s0 += t;
    t = c1[i];
    c1[i] = s1, s1 += t;
  }
  for (int i = 0; i < 1024; i++) {
    u32 t = c2[i];
    c2[i] = s2, s2 += t;
  }
  for (u32 i = 0; i < n; i++) tmp[c0[(u32)(a[i] >> 32) & 2047]++] = a[i];
  for (u32 i = 0; i < n; i++) a[c1[(u32)(tmp[i] >> 43) & 2047]++] = tmp[i];
  for (u32 i = 0; i < n; i++) tmp[c2[(u32)(a[i] >> 54)]++] = a[i];
  memcpy(a, tmp, (size_t)n * 8);
}
static AINLINE void emit_row(TC& tc, u64 person, u32 day, u32 sessions, u32 events, i64 revenue) {
  Out& o = tc.out;
  o.uid.need(64);
  if (LIKELY(is_raw(person))) {
    u32 len = 1 + raw_len(person);
    char* w = o.uid.w;
    memcpy(w, &len, 4);
    w[4] = 'u';
    memcpy(w + 5, &person, 8);
    o.uid.w += 4 + len;
  } else {
    string s = g_dict.str(person);
    o.uid.need(s.size() + 8);
    o.uid.put<u32>((u32)s.size());
    memcpy(o.uid.w, s.data(), s.size());
    o.uid.w += s.size();
  }
  o.day.need(4), o.ses.need(4), o.evs.need(4), o.rev.need(8);
  o.day.put<int32_t>((int32_t)(DAY0 + day));
  o.ses.put<u32>(sessions);
  o.evs.put<u32>(events);
  o.rev.put<i64>(revenue);
  if ((o.rows & 7) == 0) {
    o.defs.need(1);
    *o.defs.w++ = 0;
  }
  if (revenue > 0) {
    i64* cr = tc.catrev;
    i64 best = 0;
    u32 top = 0;
    for (u32 c = 0; c < NCAT; c++) {
      if (cr[c] > best) best = cr[c], top = c;
      cr[c] = 0;
    }
    const string& name = g_cat_names[top];
    o.cat.need(name.size() + 4);
    o.cat.put<u32>((u32)name.size());
    memcpy(o.cat.w, name.data(), name.size());
    o.cat.w += name.size();
    o.defs.w[-1] |= (char)(1 << (o.rows & 7));
  }
  o.rows++;
}
static AINLINE void win_add(TC& tc, u32 c) {
  if (tc.wcnt[c]++ == 0) {
    tc.wpos[c] = tc.nact;
    tc.wact[tc.nact++] = c;
  }
}
static AINLINE void win_del(TC& tc, u32 c) {
  if (--tc.wcnt[c] == 0) {
    u32 last = tc.wact[--tc.nact];
    u32 at = tc.wpos[c];
    tc.wact[at] = last;
    tc.wpos[last] = at;
  }
}
static AINLINE bool click_less(const CkItem& x, const CkItem& y) {
  if (x.ts != y.ts) return x.ts < y.ts;
  return x.cid != y.cid && id_less(x.cid, y.cid, 'k');
}
// Orders a person's clicks by (ts, click_id) and drops repeated copies. Returns how many are left.
static u32 sort_clicks(TC& tc, CkItem* ck, u32 m) {
  if (m <= 48) {
    for (u32 i = 1; i < m; i++) {
      CkItem v = ck[i];
      u32 j = i;
      while (j > 0 && click_less(v, ck[j - 1])) {
        ck[j] = ck[j - 1];
        j--;
      }
      ck[j] = v;
    }
  } else {  // by ts first, as integers; then the rare equal-ts runs by click_id
    tc.ckey.ensure((size_t)m * 2);
    u64* k = tc.ckey.p;
    for (u32 i = 0; i < m; i++) k[i] = ((u64)ck[i].ts << 32) | i;
    if (m < 2048)
      std::sort(k, k + m);
    else
      radix_hi32(k, k + m, m);
    tc.craw.ensure(m);
    CkItem* tmp = tc.craw.p;
    for (u32 i = 0; i < m; i++) tmp[i] = ck[(u32)k[i]];
    memcpy(ck, tmp, (size_t)m * sizeof(CkItem));
    for (u32 i = 0; i < m;) {
      u32 j = i + 1;
      while (j < m && ck[j].ts == ck[i].ts) j++;
      if (j - i > 1) std::sort(ck + i, ck + j, click_less);
      i = j;
    }
  }
  u32 w = 1;  // copies of a click are identical: equal ts and click_id
  for (u32 i = 1; i < m; i++)
    if (ck[i].ts != ck[w - 1].ts || ck[i].cid != ck[w - 1].cid) ck[w++] = ck[i];
  return w;
}
// ev: the person's events as (index, bucket, ts) packed high to low, ordered
// by bucket only. A bucket is 30 minutes of the window, so two events in one
// bucket are always in the same session, and events two or more buckets apart
// never are; only neighbouring buckets need their closest timestamps compared.
// pu: the partition's purchases by (index << 32 | ts); this person's are next.
static void do_person(TC& tc, u64 person, u32 li, const u64* ev, u32 n, CkItem* ck, u32 m, const Pu*& pu, const Pu* pu_end) {
  // With only a few clicks, each session just looks at all of them; otherwise
  // they are put in order and a 7-day window slides along the sessions.
  const bool few = m <= 16;
  if (!few) m = sort_clicks(tc, ck, m);
  u32 lo = 0, hi = 0;
  int curday = -1;
  u32 r_s = 0, r_e = 0;
  i64 r_rev = 0;
  i64* catrev = tc.catrev;
  const u64 top = (u64)li << 32;
  const u32 bmask = (1u << g_bbits) - 1;
  u32 i = 0;
  // the first bucket run
  u32 rb = (u32)(ev[0] >> 32) & bmask, rmin = (u32)ev[0], rmax = rmin, rcnt = 1;
  for (i = 1; i < n && ((u32)(ev[i] >> 32) & bmask) == rb; i++) {
    u32 t = (u32)ev[i];
    rmin = std::min(rmin, t), rmax = std::max(rmax, t);
    rcnt++;
  }
  for (;;) {
    // a session starts with the run in hand, and takes following runs while they are close enough
    const u32 start = rmin;
    u32 last = rmax, events = rcnt, pb = rb;
    bool more = false;
    while (i < n) {
      rb = (u32)(ev[i] >> 32) & bmask;
      rmin = rmax = (u32)ev[i];
      rcnt = 1;
      for (i++; i < n && ((u32)(ev[i] >> 32) & bmask) == rb; i++) {
        u32 t = (u32)ev[i];
        rmin = std::min(rmin, t), rmax = std::max(rmax, t);
        rcnt++;
      }
      if (rb == pb + 1 && rmin - last <= GAP_MS) {
        last = rmax, events += rcnt, pb = rb;
      } else {
        more = true;  // the run in hand opens the next session
        break;
      }
    }
    int day = (int)(((u64)start + WS_TOD) / (u64)DAYMS);
    if (day != curday) {
      if (curday >= 0) emit_row(tc, person, (u32)curday, r_s, r_e, r_rev);
      curday = day, r_s = 0, r_e = 0, r_rev = 0;
    }
    i64 rev = 0;
    const u64 lim = top | last;  // purchases up to the session's last event are its own
    while (pu < pu_end && pu->key <= lim) {
      u32 pc = pu->pc;
      rev += pc >> 8, catrev[pc & 0xFF] += pc >> 8;
      pu++;
    }
    r_s++, r_e += events, r_rev += rev;
    u32 credit = ORGANIC;
    if (few) {  // click times are measured from WS - 7 days, so the window is [start, start + 7 days]
      const size_t nd = (size_t)ND;
      u32* const stamp = tc.stamp;
      const u32 sid = ++tc.sid;
      u32 best = 0;
      u64 bestid = 0;
      bool found = false;
      for (u32 k = 0; k < m; k++) {
        const u32 t = ck[k].ts;
        if (t - start > SEVEN_MS) continue;
        const u32 c = ck[k].camp;
        if (stamp[c] != sid) stamp[c] = sid, tc.add_asst(c * nd + day);
        if (!found || t > best || (t == best && ck[k].cid != bestid && id_less(ck[k].cid, bestid, 'k'))) found = true, best = t, bestid = ck[k].cid, credit = c;
      }
    } else {
      u32 hi_lim = start + SEVEN_MS, lo_lim = start;
      while (hi < m && ck[hi].ts <= hi_lim) win_add(tc, ck[hi++].camp);
      while (lo < hi && ck[lo].ts < lo_lim) win_del(tc, ck[lo++].camp);
      if (hi > lo) {
        u32 b = hi - 1;
        while (b > lo && ck[b - 1].ts == ck[b].ts) b--;
        credit = ck[b].camp;
        const size_t nd = (size_t)ND;
        const u32* act = tc.wact;
        for (u32 k = 0, na = tc.nact; k < na; k++) tc.add_asst(act[k] * nd + day);
      }
    }
    size_t at = (size_t)credit * (size_t)ND + day;
    tc.add_sess(at);
    if (rev > 0) tc.a_purch[at]++, tc.a_rev[at] += rev;
    if (!more) break;
  }
  if (curday >= 0) emit_row(tc, person, (u32)curday, r_s, r_e, r_rev);
  while (lo < hi) win_del(tc, ck[lo++].camp);  // only the sliding window leaves anything behind
}
static NOINLINE void lt_grow(TC& tc) {
  u32 oc = tc.ltcap;
  LSlot* old = tc.lt;
  tc.ltcap = oc * 2, tc.ltbits++;
  tc.lt = (LSlot*)hot_alloc(tc.ltcap * sizeof(LSlot));
  for (u32 i = 0; i < oc; i++)
    if (old[i].key) {
      u32 s = (u32)((hash64(old[i].key) << g_p2bits) >> (64 - tc.ltbits));
      while (tc.lt[s].key) s = (s + 1) & (tc.ltcap - 1);
      tc.lt[s] = old[i];
    }
}
static vector<u64> g_p2events, g_p2clicks, g_p2fwd;  // bytes per partition: events by user ID, clicks by person, forwarded words
// The 32-bit words one thread forwarded to a partition, across its blocks.
struct WordReader {
  const Blk* b;
  const u32 *p = nullptr, *e = nullptr;
  explicit WordReader(const Blk* head) : b(head) { open(); }
  void open() {
    for (; b; b = b->next)
      if (b->used) {
        p = (const u32*)((const char*)b + BLKH), e = p + b->used / 4;
        return;
      }
    p = e = nullptr;
  }
  bool more() {
    if (p != e) return true;
    if (!b) return false;
    b = b->next;
    open();
    return p != e;
  }
  u32 next() {
    if (p == e) more();
    return *p++;
  }
};
// Sends the events and clicks of one linked ID to the partition of the person
// it belongs to: the person, three counts, every event's ts, (ts, pc) per
// purchase, (click_id, ts, campaign) per click.
static AINLINE void forward_run(TC& tc, u64 root, u32 li, const u64* ev, u32 n, const Pu*& pu, const Pu* pu_end, const CkItem* ck, u32 m) {
  const Pu* const p0 = pu;
  while (pu < pu_end && (u32)(pu->key >> 32) == li) pu++;
  auto& fw = tc.fwd;
  const u32 q = (u32)(hash64(root) >> (64 - g_p2bits));
  char* const base = fw.wc + (size_t)q * 128;
  u32 f = fw.fill[q];
  auto put = [&](u32 w) {
    memcpy(base + f, &w, 4);
    f += 4;
    if (UNLIKELY(f == 128)) {
      fw.flush(q, 128);
      f = 0;
    }
  };
  put((u32)root), put((u32)(root >> 32)), put(n), put((u32)(pu - p0)), put(m);
  for (u32 i = 0; i < n; i++) put((u32)ev[i]);
  for (const Pu* x = p0; x < pu; x++) put((u32)x->key), put(x->pc);
  for (u32 i = 0; i < m; i++) put((u32)ck[i].cid), put((u32)(ck[i].cid >> 32)), put(ck[i].ts), put(ck[i].camp);
  fw.fill[q] = (u8)f;
}
// Phase 3 runs twice over the partitions.
// First round (merged = false): partition p of the events and clicks, split
// by user ID. Each ID is looked up in the links once. People with no link are
// finished here; what a linked ID has is forwarded to its person's partition.
// Second round (merged = true): partition p of what was forwarded, by person.
static void phase3_partition(TC& tc, u32 p, bool merged) {
  const size_t N = merged ? g_p2fwd[p] / 4 : g_p2events[p] / sizeof(Ev2);      // at most this many events
  const size_t NCK = merged ? g_p2fwd[p] / 16 : g_p2clicks[p] / sizeof(Ck);  // and clicks
  if (!N && !NCK) return;
  tc.eva.ensure(N), tc.evb.ensure(N), tc.pu.ensure(N);
  tc.craw.ensure(NCK), tc.cidx.ensure(NCK), tc.ck.ensure(NCK);
  u32 nc = 0;  // clicks in this partition
  if (!tc.lt) {
    tc.ltcap = 1024, tc.ltbits = 10;
    tc.lt = (LSlot*)hot_alloc(tc.ltcap * sizeof(LSlot));
  }
  memset(tc.lt, 0, (size_t)tc.ltcap * sizeof(LSlot));
  memset(tc.hb, 0, (size_t)g_nbkt * sizeof(u32) * (merged ? 2 : 1));
  u32 np = 0, npu = 0;
  u64* A = tc.eva.p;
  u64* B = tc.evb.p;
  Pu* pu = tc.pu.p;
  const int bbits = g_bbits, p2bits = g_p2bits;
  const u32 maxli = (1u << (32 - bbits)) - 1;
  size_t NE = 0;  // events in this partition
  // Give each ID or person an index, and pack (index, bucket, ts) for every event.
  {
    u32* __restrict hb = tc.hb;
    size_t j = 0;
    LSlot* lt = tc.lt;
    u32 ltmask = tc.ltcap - 1;
    int ltsh = 64 - (int)tc.ltbits;
    u32* cnt = tc.cnt.p;
    auto index_of = [&](u64 key) -> u32 {
      u32 s = (u32)((hash64(key) << p2bits) >> ltsh);
      for (;;) {
        LSlot& sl = lt[s];
        if (LIKELY(sl.key == key)) return sl.val;
        if (!sl.key) {
          const u32 li = np++;
          sl.key = key, sl.val = li;
          tc.pkey.keep(np), tc.cnt.keep(np);
          tc.pkey[li] = key, tc.cnt[li] = 0;
          if (UNLIKELY(np * 2 > tc.ltcap)) lt_grow(tc);
          lt = tc.lt, ltmask = tc.ltcap - 1, ltsh = 64 - (int)tc.ltbits, cnt = tc.cnt.p;
          return li;
        }
        s = (s + 1) & ltmask;
      }
    };
    if (!merged) {
      for (int t = 0; t < NT; t++)
        for (Blk* b = g_tc[t]->pes.head[p]; b; b = b->next) {
          const Ev2* r = (const Ev2*)((char*)b + BLKH);
          const u32 n = b->used / (u32)sizeof(Ev2);
          for (u32 k = 0; k < n; k++) {
            const u32 li = index_of(r[k].uid);
            const u64 tspc = r[k].tspc;
            const u32 ts = (u32)(tspc >> 32), pc = (u32)tspc;
            const u32 bk = ts / GAP_MS;
            A[j++] = ((u64)((li << bbits) | bk) << 32) | ts;
            hb[bk]++;
            cnt[li]++;
            if (UNLIKELY(pc != 0)) pu[npu++] = Pu{((u64)li << 32) | ts, pc, 0};
          }
        }
      CkItem* const craw = tc.craw.p;
      u32* const cidx = tc.cidx.p;
      for (int t = 0; t < NT; t++)
        for (Blk* b = g_tc[t]->cks.head[p]; b; b = b->next) {
          const Ck* r = (const Ck*)((char*)b + BLKH);
          const u32 n = b->used / (u32)sizeof(Ck);
          for (u32 k = 0; k < n; k++) {
            cidx[nc] = index_of(r[k].uid);  // an ID with clicks and no events gets an index too: it may be linked
            craw[nc++] = CkItem{r[k].cid, r[k].ts, r[k].camp};
          }
        }
    } else {
      CkItem* const craw = tc.craw.p;
      u32* const cidx = tc.cidx.p;
      for (int t = 0; t < NT; t++) {
        WordReader rd(g_tc[t]->fwd.head[p]);
        while (rd.more()) {
          u64 root = rd.next();
          root |= (u64)rd.next() << 32;
          u32 n = rd.next();
          const u32 k = rd.next();
          const u32 m = rd.next();
          const u32 li = index_of(root);
          cnt[li] += n;
          const u64 top = (u64)li << bbits;
          while (n) {
            rd.more();
            const u32 c = std::min(n, (u32)(rd.e - rd.p));
            const u32* __restrict w = rd.p;
            for (u32 i = 0; i < c; i++) {  // a run comes in bucket order: its neighbours share buckets
              const u32 ts = w[i];
              const u32 bk = ts / GAP_MS;
              A[j++] = ((top | bk) << 32) | ts;
              hb[(bk << 1) | (i & 1)]++;
            }
            rd.p += c, n -= c;
          }
          for (u32 i = 0; i < k; i++) {
            const u32 ts = rd.next();
            const u32 pc = rd.next();
            pu[npu++] = Pu{((u64)li << 32) | ts, pc, 0};
          }
          for (u32 i = 0; i < m; i++) {
            u64 cid = rd.next();
            cid |= (u64)rd.next() << 32;
            const u32 ts = rd.next();
            const u32 camp = rd.next();
            cidx[nc] = li;
            craw[nc++] = CkItem{cid, ts, camp};
          }
        }
      }
    }
    NE = j;
    if (merged)
      for (u32 i = 0, nb = g_nbkt; i < nb; i++) hb[i] = hb[2 * i] + hb[2 * i + 1];
  }
  if (UNLIKELY(np > maxli)) {  // the packing above has no room for this many people
    g_bail.store(true, std::memory_order_relaxed);
    return;
  }
  // First round: which of these IDs are in a link, and whose are they. The
  // table is far larger than the cache, so each slot is fetched a few IDs ahead.
  u8* plink = nullptr;
  if (!merged) {
    tc.plink.ensure(np);
    plink = tc.plink.p;
    const IdSlot* const ids = g_ids;
    const u64 idcap = g_idcap;
    u64* const pkey = tc.pkey.p;
    const u32 PD = 16;
    for (u32 i = 0, e = std::min(np, PD); i < e; i++) __builtin_prefetch(&ids[mulhi(hash64(pkey[i]), idcap)]);
    for (u32 i = 0; i < np; i++) {
      if (LIKELY(i + PD < np)) __builtin_prefetch(&ids[mulhi(hash64(pkey[i + PD]), idcap)]);
      const u64 uid = pkey[i];
      u64 s = mulhi(hash64(uid), idcap);
      u8 linked = 0;
      for (;;) {
        const IdSlot& sl = ids[s];
        if (sl.key == uid) {
          pkey[i] = sl.val, linked = 1;
          break;
        }
        if (LIKELY(!sl.key)) break;
        if (UNLIKELY(++s == idcap)) s = 0;
      }
      plink[i] = linked;
    }
  }
  // Order by (index, bucket): one counting pass on each.
  tc.pos.ensure(np + 1), tc.ccnt.ensure(np + 1), tc.cpos.ensure(np + 1);
  if (NE < 128) {
    std::sort(A, A + NE);
  } else {
    u32* __restrict hb = tc.hb;
    u32* __restrict ps = tc.pos.p;
    u64* __restrict a = A;
    u64* __restrict b = B;
    u32 acc = 0;
    for (u32 i = 0, nb = g_nbkt; i < nb; i++) {
      u32 c = hb[i];
      hb[i] = acc, acc += c;
    }
    acc = 0;
    for (u32 i = 0; i < np; i++) ps[i] = acc, acc += tc.cnt[i];
    const u32 bmask = (1u << bbits) - 1;
    const int lsh = 32 + bbits;
    for (size_t i = 0; i < NE; i++) {
      const u64 v = a[i];
      b[hb[(u32)(v >> 32) & bmask]++] = v;
    }
    for (size_t i = 0; i < NE; i++) {
      const u64 v = b[i];
      a[ps[v >> lsh]++] = v;
    }
  }
  if (npu > 1) std::sort(pu, pu + npu, [](const Pu& x, const Pu& y) { return x.key < y.key; });
  // Group the clicks by index.
  for (u32 i = 0; i < np; i++) tc.ccnt[i] = 0;
  if (nc) {
    for (u32 i = 0; i < nc; i++) tc.ccnt[tc.cidx[i]]++;
    u32 acc = 0;
    for (u32 i = 0; i < np; i++) tc.cpos[i] = acc, acc += tc.ccnt[i];
    for (u32 i = 0; i < nc; i++) tc.ck[tc.cpos[tc.cidx[i]]++] = tc.craw[i];
  }
  const Pu* pc = pu;
  const Pu* pe = pu + npu;
  u32 e0 = 0, c0 = 0;
  const int fsh = 64 - g_p2bits;
  for (u32 i = 0; i < np; i++) {
    u32 n = tc.cnt[i], m = tc.ccnt[i];
    if (plink && i + 4 < np) {  // the buffer the fourth person from here may be forwarded to
      const u32 q = (u32)(hash64(tc.pkey[i + 4]) >> fsh);
      __builtin_prefetch(tc.fwd.wc + (size_t)q * 128 + tc.fwd.fill[q], 1);
    }
    if (plink && plink[i])
      forward_run(tc, tc.pkey[i], i, A + e0, n, pc, pe, tc.ck.p + c0, m);
    else if (n)  // clicks alone make no session
      do_person(tc, tc.pkey[i], i, A + e0, n, m ? tc.ck.p + c0 : nullptr, m, pc, pe);
    e0 += n, c0 += m;
  }
  if (!merged)  // this partition's blocks carry what is forwarded from here on
    for (int t = 0; t < NT; t++)
      for (Blk** h : {&g_tc[t]->pes.head[p], &g_tc[t]->cks.head[p]}) {
        Blk* b = *h;
        *h = nullptr;
        while (b) {
          Blk* nx = b->next;
          b->next = tc.freelist;
          tc.freelist = b;
          b = nx;
        }
      }
}

// ---------------------------------------------------------------- parquet
struct TW {  // thrift compact writer
  string b;
  vector<int> last{0};
  void byte(u8 x) { b.push_back((char)x); }
  void varint(u64 v) {
    while (v >= 0x80) {
      b.push_back((char)(u8)(v | 0x80));
      v >>= 7;
    }
    b.push_back((char)(u8)v);
  }
  static u64 zz(i64 v) { return ((u64)v << 1) ^ (u64)(v >> 63); }
  void fh(int id, int type) {
    int d = id - last.back();
    if (d > 0 && d <= 15)
      byte((u8)((d << 4) | type));
    else {
      byte((u8)type);
      varint(zz(id));
    }
    last.back() = id;
  }
  void i32f(int id, int32_t v) { fh(id, 5), varint(zz(v)); }
  void i64f(int id, i64 v) { fh(id, 6), varint(zz(v)); }
  void strf(int id, const string& s) {
    fh(id, 8);
    varint(s.size());
    b.append(s);
  }
  void begin_struct(int id) {
    fh(id, 12);
    last.push_back(0);
  }
  void end_struct() {
    byte(0);
    last.pop_back();
  }
  void list(int id, int etype, size_t n) {
    fh(id, 9);
    if (n < 15)
      byte((u8)((n << 4) | etype));
    else {
      byte((u8)(0xF0 | etype));
      varint(n);
    }
  }
  void list_begin_elem() { last.push_back(0); }
  void list_end_elem() {
    byte(0);
    last.pop_back();
  }
};
enum PT { P_INT32 = 1, P_INT64 = 2, P_BYTES = 6 };
struct PqCol {
  string name;
  int ptype;
  int conv;      // -1 none, 0 UTF8, 6 DATE
  int logical;   // 0 none, 1 STRING, 6 DATE
  bool optional;
  const char* data;
  size_t bytes;
  string levels;       // definition levels, with their length prefix
  vector<iovec> segs;  // the values in pieces, when data is null
};
static void write_parquet(const string& path, vector<PqCol>& cols, i64 nrows) {
  vector<string> heads(cols.size());
  vector<iovec> iov;
  static const char MAGIC[] = "PAR1";
  iov.push_back({(void*)MAGIC, 4});
  i64 off = 4;
  vector<std::pair<i64, i64>> metas;
  for (size_t i = 0; i < cols.size(); i++) {
    PqCol& c = cols[i];
    size_t page = c.levels.size() + c.bytes;
    TW h;
    h.i32f(1, 0);  // DATA_PAGE
    h.i32f(2, (int32_t)page);
    h.i32f(3, (int32_t)page);
    h.begin_struct(5);
    h.i32f(1, (int32_t)nrows);
    h.i32f(2, 0);  // PLAIN
    h.i32f(3, 3);  // RLE
    h.i32f(4, 3);  // RLE
    h.end_struct();
    h.byte(0);
    heads[i] = h.b;
    iov.push_back({(void*)heads[i].data(), heads[i].size()});
    if (!c.levels.empty()) iov.push_back({(void*)c.levels.data(), c.levels.size()});
    if (c.data) {
      if (c.bytes) iov.push_back({(void*)c.data, c.bytes});
    } else {
      for (auto& sg : c.segs) iov.push_back(sg);
    }
    i64 size = (i64)(heads[i].size() + page);
    metas.emplace_back(off, size);
    off += size;
  }
  TW m;
  m.i32f(1, 1);
  m.list(2, 12, cols.size() + 1);
  m.list_begin_elem();
  m.strf(4, "schema");
  m.i32f(5, (int32_t)cols.size());
  m.list_end_elem();
  for (auto& c : cols) {
    m.list_begin_elem();
    m.i32f(1, c.ptype);
    m.i32f(3, c.optional ? 1 : 0);
    m.strf(4, c.name);
    if (c.conv >= 0) m.i32f(6, c.conv);
    if (c.logical) {
      m.begin_struct(10);
      m.begin_struct(c.logical);
      m.end_struct();
      m.end_struct();
    }
    m.list_end_elem();
  }
  m.i64f(3, nrows);
  m.list(4, 12, 1);
  m.list_begin_elem();
  m.list(1, 12, cols.size());
  i64 total = 0;
  for (size_t i = 0; i < cols.size(); i++) {
    auto& c = cols[i];
    m.list_begin_elem();
    m.i64f(2, metas[i].first);
    m.begin_struct(3);
    m.i32f(1, c.ptype);
    m.list(2, 5, 2);
    m.varint(TW::zz(0));
    m.varint(TW::zz(3));
    m.list(3, 8, 1);
    m.varint(c.name.size());
    m.b.append(c.name);
    m.i32f(4, 0);
    m.i64f(5, nrows);
    m.i64f(6, metas[i].second);
    m.i64f(7, metas[i].second);
    m.i64f(9, metas[i].first);
    m.end_struct();
    m.list_end_elem();
    total += metas[i].second;
  }
  m.i64f(2, total);
  m.i64f(3, nrows);
  m.list_end_elem();
  m.strf(6, "sessions-cpp 2.4 (build 0d365338c9c2)");
  m.byte(0);
  u32 ml = (u32)m.b.size();
  m.b.append((const char*)&ml, 4);
  m.b.append("PAR1", 4);
  iov.push_back({(void*)m.b.data(), m.b.size()});
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    perror(path.c_str());
    exit(1);
  }
  size_t i = 0;
  while (i < iov.size()) {
    int cnt = (int)std::min<size_t>(iov.size() - i, 512);
    ssize_t w = writev(fd, &iov[i], cnt);
    if (w < 0) {
      perror("writev");
      exit(1);
    }
    while (i < iov.size() && (size_t)w >= iov[i].iov_len) w -= (ssize_t)iov[i].iov_len, i++;
    if (w > 0) {
      iov[i].iov_base = (char*)iov[i].iov_base + w;
      iov[i].iov_len -= (size_t)w;
    }
  }
  close(fd);
}
static void write_user_daily(TC& tc, const string& dir) {
  Out& o = tc.out;
  vector<PqCol> cols;
  auto add = [&](const char* name, int ptype, int conv, int logical, bool optional, Chain& c, const string& levels) {
    const size_t bytes = c.finish();
    cols.push_back({name, ptype, conv, logical, optional, nullptr, bytes, levels, {}});
    cols.back().segs = std::move(c.segs);
  };
  // One bit-packed run of definition levels, bit width 1.
  TW t;
  size_t groups = (o.rows + 7) / 8;
  t.varint((groups << 1) | 1);
  string lv;
  u32 ln = (u32)(t.b.size() + groups);
  lv.append((const char*)&ln, 4);
  lv.append(t.b);
  o.defs.finish();
  for (auto& sg : o.defs.segs) lv.append((const char*)sg.iov_base, sg.iov_len);
  add("user_id", P_BYTES, 0, 1, false, o.uid, "");
  add("day", P_INT32, 6, 6, false, o.day, "");
  add("sessions", P_INT32, -1, 0, false, o.ses, "");
  add("events", P_INT32, -1, 0, false, o.evs, "");
  add("revenue_cents", P_INT64, -1, 0, false, o.rev, "");
  add("top_category", P_BYTES, 0, 1, true, o.cat, lv);
  write_parquet(dir + "/part-" + std::to_string(tc.id) + ".parquet", cols, (i64)o.rows);
}

// ---------------------------------------------------------------- main
static string env(const char* k, const char* d) {
  const char* v = getenv(k);
  return v && *v ? string(v) : string(d);
}
static string read_all(const string& path) {
  string s;
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) return s;
  struct stat st;
  if (fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0) {  // the usual case: one read of the whole file
    s.resize((size_t)st.st_size);
    size_t got = 0;
    while (got < s.size()) {
      ssize_t r = read(fd, &s[got], s.size() - got);
      if (r <= 0) break;
      got += (size_t)r;
    }
    s.resize(got);
  }
  char buf[65536];
  ssize_t r;
  while ((r = read(fd, buf, sizeof buf)) > 0) s.append(buf, (size_t)r);
  close(fd);
  return s;
}
static bool manifest_ts(const string& js, const char* key, i64& v) {
  size_t k = js.find(string("\"") + key + "\"");
  if (k == string::npos) return false;
  size_t c = js.find(':', k);
  if (c == string::npos) return false;
  size_t q1 = js.find('"', c + 1);
  if (q1 == string::npos) return false;
  size_t q2 = js.find('"', q1 + 1);
  if (q2 == string::npos) return false;
  return parse_ts(js.data() + q1 + 1, (u32)(q2 - q1 - 1), v) && v != TS_HUGE;
}
template <class F>
static void for_lines(const string& s, F&& f) {  // data lines of a small file
  size_t a = s.find('\n');
  if (a == string::npos) return;
  a++;
  while (a < s.size()) {
    size_t b = s.find('\n', a);
    if (b == string::npos) b = s.size();
    f(s.data() + a, s.data() + b);
    a = b + 1;
  }
}
struct Task {
  u32 file;
  size_t s, e;
};

int main(int argc, char** argv) {
  string data = env("DATA_DIR", "/data"), outdir = env("OUTPUT_DIR", "/output");
  if (argc >= 3) data = argv[argc - 2], outdir = argv[argc - 1];
  NT = atoi(env("NCPU", "0").c_str());
  if (NT <= 0) NT = (int)std::thread::hardware_concurrency();
  if (NT <= 0) NT = 8;
  if (NT > 64) NT = 64;
  VERBOSE = getenv("VERBOSE") != nullptr;
  g_hugepages = getenv("NOHUGE") == nullptr;
  g_pin = getenv("PIN") != nullptr;
  if (const char* r = getenv("RELEASE")) g_release = atoi(r);
#if defined(__x86_64__)
  if (!getenv("NOAVX512") && __builtin_cpu_supports("avx512vbmi2") && __builtin_cpu_supports("avx512vbmi") && __builtin_cpu_supports("avx512bw") && __builtin_cpu_supports("avx512vl") &&
      __builtin_cpu_supports("avx512dq"))
    g_stage1 = g_stage1_events = stage1_vbmi2;
#endif
  auto fallback = [&]() -> int {
    if (VERBOSE) fprintf(stderr, "fallback pipeline\n");
    int rc = fallback_main(data, outdir);
    fflush(stderr);
    _exit(rc);
  };
  if (getenv("FORCE_FALLBACK")) return fallback();
  g_pool.start(NT);

  string man = read_all(data + "/manifest.json");
  if (!manifest_ts(man, "window_start", WS) || !manifest_ts(man, "window_end", WE)) return fallback();
  // The compact records hold times as 32-bit offsets from a whole-second window start.
  if (WS < 0 || WS % 1000 != 0 || WE <= WS || WE - WS > 3000000000ll) return fallback();
  WLEN = (u64)(WE - WS);
  CWLEN = WLEN + SEVEN_MS;
  DAY0 = WS / DAYMS;
  WS_TOD = (u32)(WS - DAY0 * DAYMS);
  ND = (int)((WE - 1) / DAYMS - DAY0 + 1);
  g_nbkt = (u32)((WLEN - 1) / GAP_MS) + 1;
  g_bbits = 1;
  while ((1u << g_bbits) < g_nbkt) g_bbits++;
  init_dates();

  // ---- inputs: listed and mapped on another thread while the lookups load
  vector<string> in_names[3];
  vector<MFile> in_maps[3];
  std::thread mapper([&] {
    const char* dirs[3] = {"/events", "/clicks", "/links"};
    for (int k = 0; k < 3; k++) in_names[k] = list_csv(data + dirs[k]), in_maps[k].resize(in_names[k].size());
    std::atomic<size_t> next{0};
    const size_t total = in_names[0].size() + in_names[1].size() + in_names[2].size();
    parallel(std::min<int>(NT, (int)total / 8 + 1), [&](int) {
      for (;;) {
        size_t i = next.fetch_add(1);
        if (i >= total) break;
        int k = i < in_names[0].size() ? 0 : (i < in_names[0].size() + in_names[1].size() ? 1 : 2);
        size_t j = i - (k == 0 ? 0 : (k == 1 ? in_names[0].size() : in_names[0].size() + in_names[1].size()));
        in_maps[k][j] = map_file(in_names[k][j]);
      }
    });
  });

  // ---- products and campaigns
  {
    string s = read_all(data + "/products.csv");
    std::unordered_map<std::string_view, u32> cats;
    struct {  // the category of the last product that hashed here: most lookups end at it
      const char* p = nullptr;
      u32 n = 0, ci = 0;
    } recent[64];
    vector<string> names;
    struct P {
      const char* p;
      u32 n, cat;
    };
    vector<P> prods;
    for_lines(s, [&](const char* a, const char* b) {
      const char* fp[2];
      u32 fl[2];
      if (!split(a, b, 2, fp, fl)) return;
      std::string_view cn(fp[1], fl[1]);  // points into s, which outlives the map
      auto& rc = recent[(fl[1] * 7 + (fl[1] ? (u8)fp[1][0] + (u8)fp[1][fl[1] - 1] * 5 : 0)) & 63];
      u32 ci;
      if (rc.p && rc.n == fl[1] && memcmp(rc.p, fp[1], fl[1]) == 0) {
        ci = rc.ci;
      } else {
        auto it = cats.find(cn);
        if (it == cats.end()) {
          ci = (u32)names.size();
          cats.emplace(cn, ci);
          names.emplace_back(cn);
        } else {
          ci = it->second;
        }
        rc.p = fp[1], rc.n = fl[1], rc.ci = ci;
      }
      prods.push_back({fp[0], fl[0], ci});
    });
    if (!cats.count(std::string_view("unknown"))) names.push_back("unknown");
    NCAT = (u32)names.size();
    if (NCAT > 255) return fallback();
    vector<u32> order(NCAT), rank(NCAT);
    for (u32 i = 0; i < NCAT; i++) order[i] = i;
    std::sort(order.begin(), order.end(), [&](u32 x, u32 y) { return names[x] < names[y]; });
    g_cat_names.resize(NCAT);
    for (u32 r = 0; r < NCAT; r++) rank[order[r]] = r, g_cat_names[r] = names[order[r]];
    UNKNOWN_CAT = (u32)(std::find(g_cat_names.begin(), g_cat_names.end(), "unknown") - g_cat_names.begin());
    g_prod.init(prods.size());
    g_prodn.init("p");
    for (auto& p : prods) add_id(g_prodn, g_prod, p.p, p.n, rank[p.cat]);  // the first of repeated IDs counts
  }
  {
    string s = read_all(data + "/campaigns.csv");
    size_t lines = (size_t)std::count(s.begin(), s.end(), '\n') + 1;
    g_camp.init(lines);
    g_campn.init("c_");
    for_lines(s, [&](const char* a, const char* b) {
      const char* fp[2];
      u32 fl[2];
      if (!split(a, b, 2, fp, fl)) return;
      if (add_id(g_campn, g_camp, fp[0], fl[0], (u32)g_camp_id.size())) g_camp_id.emplace_back(fp[0], fl[0]), g_camp_chan.emplace_back(fp[1], fl[1]);
    });
    NCAMP = (u32)g_camp_id.size();
    ORGANIC = NCAMP;
    if (NCAMP > 1000000) return fallback();
  }
  tick("lookups");

  // ---- inputs
  vector<MFile> ev_files, ck_files;
  vector<Task> ev_tasks, ck_tasks;
  size_t ev_bytes = 0, ck_bytes = 0;
  const size_t CHUNK = (size_t)atoi(env("CHUNK_KB", "1024").c_str()) << 10;
  {
    mapper.join();
    vector<MFile>* const maps = in_maps;
    for (auto& f : maps[0])
      if (f.p) {
        for (size_t s = 0; s < f.n; s += CHUNK) ev_tasks.push_back({(u32)ev_files.size(), s, std::min(f.n, s + CHUNK)});
        ev_files.push_back(f), ev_bytes += f.n;
      }
    for (auto& f : maps[1])
      if (f.p) {
        for (size_t s = 0; s < f.n; s += CHUNK) ck_tasks.push_back({(u32)ck_files.size(), s, std::min(f.n, s + CHUNK)});
        ck_files.push_back(f), ck_bytes += f.n;
      }
    for (auto& f : maps[2])
      if (f.p) g_link_files.push_back(f);
  }
  auto pbits = [&](const char* name, size_t bytes) {
    int forced = atoi(env(name, "0").c_str());
    if (forced > 0) return forced;
    int b = 4;
    while (b < 11 && ((size_t)800 << 10 << b) < bytes) b++;
    return b;
  };
  g_p1bits = pbits("P1BITS", ev_bytes);
  g_p2bits = pbits("P2BITS", ev_bytes);
  BATCH = (u32)atoi(env("BATCH_KB", "64").c_str()) << 10;
  const u32 P1 = 1u << g_p1bits, P2 = 1u << g_p2bits;
  g_tc.resize(NT);
  tick("mapped");
#ifdef __linux__
  if (VERBOSE && getenv("INCORE")) {  // how much of the input is already in the page cache
    size_t pages = 0, in = 0;
    vector<unsigned char> vec;
    for (auto* fs : {&ev_files, &ck_files})
      for (auto& f : *fs) {
        size_t np = (f.n + 4095) / 4096;
        vec.assign(np, 0);
        if (mincore((void*)f.p, f.n, vec.data()) == 0) {
          pages += np;
          for (unsigned char c : vec) in += c & 1;
        }
      }
    fprintf(stderr, "[%8.1f ms] incore: %zu of %zu pages\n", now_ms(), in, pages);
  }
#endif

#ifdef BENCH
  if (const char* bf = getenv("BENCH_FILE")) {  // single-thread parse benchmark, no kernel in the loop
    string raw = read_all(bf);
    size_t n = raw.size();
    char* buf = (char*)calloc(1, n + 65536);
    memcpy(buf + 4096, raw.data(), n);
    MFile mf{buf + 4096, n};
    g_idcap = 64, g_ids = (IdSlot*)calloc(64, sizeof(IdSlot));
    Kind kind = getenv("BENCH_CLICKS") ? K_CLICKS : K_EVENTS;
    for (int it = 0; it < (getenv("BENCH_ITERS") ? atoi(getenv("BENCH_ITERS")) : 6); it++) {
      TC* tcp = new TC();
      TC& tc = *tcp;
      tc.evs.init(P1, &tc.arena, &tc.freelist);
      tc.cks.init(P2, &tc.arena, &tc.freelist);
      tc.ix.init(BATCH);
      double t0 = now_ms();
      u64 nlsum = 0;
      for (size_t pos = 0; pos < n; pos += BATCH) {
        g_stage1(mf.p + pos, (u32)std::min<size_t>(BATCH, n - pos), tc.ix);
        nlsum += tc.ix.nnl;
      }
      double t1 = now_ms();
      parse_chunk(tc, mf, 0, n, kind);
      double t2 = now_ms();
      fprintf(stderr, "bench: %llu lines  stage1 %.1f ns/line (%.2f ns/B)  full %.1f ns/line  slow %llu\n", (unsigned long long)nlsum, (t1 - t0) * 1e6 / nlsum, (t1 - t0) * 1e6 / n, (t2 - t1) * 1e6 / nlsum,
              (unsigned long long)tc.n_slow);
    }
    _exit(0);
  }
#endif
  // ---- phase 1: parse
  std::atomic<size_t> ev_next{0}, ck_next{0};
  std::atomic<bool> links_done{false};
  parallel(NT, [&](int t) {
    TC* tcp = new TC();
    TC& tc = *tcp;
    g_tc[t] = tcp;
    tc.id = t;
    t_hot = &tc.hot;
    tc.evs.init(P1, &tc.arena, &tc.freelist);
    tc.pes.init(P2, &tc.arena, &tc.freelist);
    tc.cks.init(P2, &tc.arena, &tc.freelist);
    tc.ix.init(BATCH);
    if (t == 0) {
      do_links(tc);
      links_done.store(true);
      if (VERBOSE) fprintf(stderr, "[%8.1f ms] links done: %zu pairs\n", now_ms(), tc.pairs.size());
    }
    for (;;) {
      size_t i = ev_next.fetch_add(1, std::memory_order_relaxed);
      if (i >= ev_tasks.size()) break;
      parse_chunk(tc, ev_files[ev_tasks[i].file], ev_tasks[i].s, ev_tasks[i].e, K_EVENTS);
      release_chunk(ev_files[ev_tasks[i].file], ev_tasks[i].s, ev_tasks[i].e);
    }
    tc.evs.finish();
    tc.t_ev = now_ms();
    for (;;) {
      size_t i = ck_next.fetch_add(1, std::memory_order_relaxed);
      if (i >= ck_tasks.size()) break;
      parse_chunk(tc, ck_files[ck_tasks[i].file], ck_tasks[i].s, ck_tasks[i].e, K_CLICKS);
      release_chunk(ck_files[ck_tasks[i].file], ck_tasks[i].s, ck_tasks[i].e);
    }
    tc.cks.finish();
    tc.t_ck = now_ms();
  });
  if (g_bail.load()) return fallback();
  if (VERBOSE) {
    u64 lines = 0, slow = 0;
    for (auto* tc : g_tc) lines += tc->n_lines, slow += tc->n_slow;
    double e0 = 1e18, e1 = 0;
    for (auto* tc : g_tc) e0 = std::min(e0, tc->t_ev), e1 = std::max(e1, tc->t_ev);
    fprintf(stderr, "[%8.1f ms] events parsed between %.1f and %.1f ms\n", now_ms(), e0, e1);
    fprintf(stderr, "[%8.1f ms] parsed: %llu lines, %llu slow, P1=%u P2=%u\n", now_ms(), (unsigned long long)lines, (unsigned long long)slow, P1, P2);
  }

  // ---- phase 2: dedup, person, scatter by person
  {
    // The input is not needed again: unmap it now, shared among the threads,
    // rather than leave it all to process exit on one core.
    vector<std::pair<char*, size_t>> pieces;
#ifdef __linux__
    if (g_release) {
      const size_t PIECE = 8u << 20;
      for (auto* fs : {&ev_files, &ck_files, &g_link_files})
        for (auto& f : *fs) {
          const size_t len = (f.n + 4095) & ~(size_t)4095;
          for (size_t o = 0; o < len; o += PIECE) pieces.emplace_back((char*)f.p + o, std::min(PIECE, len - o));
        }
    }
#endif
    std::atomic<size_t> rel{0};
    std::atomic<u32> next{0};
    parallel(NT, [&](int t) {
      (void)rel;
#ifdef __linux__
      for (;;) {
        size_t i = rel.fetch_add(1, std::memory_order_relaxed);
        if (i >= pieces.size()) break;
        madvise(pieces[i].first, pieces[i].second, MADV_DONTNEED);
      }
#endif
      TC& tc = *g_tc[t];
      t_hot = &tc.hot;
      for (;;) {
        u32 p = next.fetch_add(1, std::memory_order_relaxed);
        if (p >= P1) break;
        phase2_partition(tc, p);
      }
      tc.pes.finish();
    });
  }
  tick("dedup");

  // ---- phase 3: sessions, attribution, user_daily
  string udd = outdir + "/user_daily", cdd = outdir + "/campaign_daily";
  mkdir(outdir.c_str(), 0777);
  mkdir(udd.c_str(), 0777);
  mkdir(cdd.c_str(), 0777);
  g_p2events.assign(P2, 0), g_p2clicks.assign(P2, 0);
  for (auto* tc : g_tc)
    for (u32 p = 0; p < P2; p++) g_p2events[p] += tc->pes.nbytes[p], g_p2clicks[p] += tc->cks.nbytes[p];
  vector<u32> order(P2);
  for (u32 p = 0; p < P2; p++) order[p] = p;
  std::sort(order.begin(), order.end(), [&](u32 a, u32 b) { return g_p2events[a] > g_p2events[b]; });
  const size_t NAGG = (size_t)(NCAMP + 1) * ND;
  {
    std::atomic<u32> next{0};
    parallel(NT, [&](int t) {
      TC& tc = *g_tc[t];
      t_hot = &tc.hot;
      tc.wcnt = (u32*)hot_alloc((NCAMP + 1) * 4), tc.wpos = (u32*)hot_alloc((NCAMP + 1) * 4), tc.wact = (u32*)hot_alloc((NCAMP + 1) * 4);
      tc.stamp = (u32*)hot_alloc((NCAMP + 1) * 4);
      tc.catrev = (i64*)hot_alloc(256 * 8);
      tc.agg = (TC::Agg*)hot_alloc(NAGG * sizeof(TC::Agg)), tc.agg8 = (TC::Agg8*)hot_alloc(NAGG * sizeof(TC::Agg8));
      tc.a_purch = (u32*)hot_alloc(NAGG * 4);
      tc.a_rev = (i64*)hot_alloc(NAGG * 8);
      tc.fwd.init(P2, &tc.arena, &tc.freelist);
      tc.out.init(&tc.arena, &tc.freelist);
      for (;;) {
        u32 i = next.fetch_add(1, std::memory_order_relaxed);
        if (i >= P2) break;
        phase3_partition(tc, order[i], false);
      }
      tc.fwd.finish();
    });
  }
  if (g_bail.load()) return fallback();
  tick("people without links");
  g_p2fwd.assign(P2, 0);
  for (auto* tc : g_tc)
    for (u32 p = 0; p < P2; p++) g_p2fwd[p] += tc->fwd.nbytes[p];
  std::sort(order.begin(), order.end(), [&](u32 a, u32 b) { return g_p2fwd[a] > g_p2fwd[b]; });
  {
    std::atomic<u32> next{0};
    parallel(NT, [&](int t) {
      TC& tc = *g_tc[t];
      t_hot = &tc.hot;
      for (;;) {
        u32 i = next.fetch_add(1, std::memory_order_relaxed);
        if (i >= P2) break;
        phase3_partition(tc, order[i], true);
      }
      if (tc.out.rows || t == 0) write_user_daily(tc, udd);
    });
  }
  if (g_bail.load()) return fallback();
  tick("sessions");

  // ---- campaign_daily
  {
    struct Tot {
      i64 s, as, pu, rv;
    };
    // The table of totals and the seven columns have known upper bounds. No
    // block is live any more, so they go in arena memory that is already
    // paged in rather than in fresh pages the kernel would have to zero.
    size_t idmax = 7, chmax = 7;  // "organic"
    for (auto& x : g_camp_id) idmax = std::max(idmax, x.size());
    for (auto& x : g_camp_chan) chmax = std::max(chmax, x.size());
    const size_t colcap[7] = {NAGG * (idmax + 4), NAGG * (chmax + 4), NAGG * 4, NAGG * 8, NAGG * 8, NAGG * 8, NAGG * 8};
    size_t want = NAGG * sizeof(Tot);
    for (size_t c : colcap) want += (c + 15) & ~(size_t)15;
    char* const warm = g_tc[0]->arena.paged_in(want);
    vector<Tot> tot_heap(warm ? 0 : NAGG);
    Tot* const tot = warm ? (Tot*)warm : tot_heap.data();
    {
      std::atomic<size_t> next{0};
      const size_t STEP = 4096;
      parallel(NT, [&](int) {
        for (;;) {
          const size_t lo = next.fetch_add(STEP, std::memory_order_relaxed);
          if (lo >= NAGG) break;
          const size_t hi = std::min(NAGG, lo + STEP);
          for (size_t at = lo; at < hi; at++) {
            i64 s = 0, pu = 0, rv = 0, as = 0;
            for (auto* tc : g_tc)
              s += tc->agg[at].sess + tc->agg8[at].sess, pu += tc->a_purch[at], rv += tc->a_rev[at], as += tc->agg[at].asst + tc->agg8[at].asst;
            tot[at] = Tot{s, as, pu, rv};
          }
        }
      });
    }
    Buf cid, chan, day, ses, pur, rev, ast;
    if (warm) {
      char* w = warm + NAGG * sizeof(Tot);
      int k = 0;
      for (Buf* b : {&cid, &chan, &day, &ses, &pur, &rev, &ast}) {
        b->p = w, b->cap = colcap[k], b->lent = true;
        w += (colcap[k++] + 15) & ~(size_t)15;
      }
    }
    i64 n = 0;
    const string organic = "organic";
    for (u32 c = 0; c <= NCAMP; c++) {
      const string& id = c == ORGANIC ? organic : g_camp_id[c];
      const string& ch = c == ORGANIC ? organic : g_camp_chan[c];
      for (int d = 0; d < ND; d++) {
        const Tot& t = tot[(size_t)c * (size_t)ND + d];
        const i64 s = t.s, pu = t.pu, rv = t.rv, as = t.as;
        if (!s && !as) continue;
        cid.need(id.size() + 4), chan.need(ch.size() + 4);
        cid.put<u32>((u32)id.size());
        memcpy(cid.p + cid.n, id.data(), id.size()), cid.n += id.size();
        chan.put<u32>((u32)ch.size());
        memcpy(chan.p + chan.n, ch.data(), ch.size()), chan.n += ch.size();
        day.need(4), ses.need(8), pur.need(8), rev.need(8), ast.need(8);
        day.put<int32_t>((int32_t)(DAY0 + d));
        ses.put<i64>(s), pur.put<i64>(pu), rev.put<i64>(rv), ast.put<i64>(as);
        n++;
      }
    }
    vector<PqCol> cols;
    cols.push_back({"campaign_id", P_BYTES, 0, 1, false, cid.p, cid.n, ""});
    cols.push_back({"channel", P_BYTES, 0, 1, false, chan.p, chan.n, ""});
    cols.push_back({"day", P_INT32, 6, 6, false, day.p, day.n, ""});
    cols.push_back({"sessions", P_INT64, -1, 0, false, ses.p, ses.n, ""});
    cols.push_back({"purchasing_sessions", P_INT64, -1, 0, false, pur.p, pur.n, ""});
    cols.push_back({"revenue_cents", P_INT64, -1, 0, false, rev.p, rev.n, ""});
    cols.push_back({"assisted_sessions", P_INT64, -1, 0, false, ast.p, ast.n, ""});
    write_parquet(cdd + "/part-0.parquet", cols, n);
  }
  tick("written");
#ifdef __linux__
  if (g_release) {  // give the big regions back from every thread, in 8 MB pieces
    vector<std::pair<char*, size_t>> pieces;
    const size_t PIECE = 8u << 20;
    for (auto& r : g_regions)
      for (size_t o = 0; o < r.second; o += PIECE) pieces.emplace_back(r.first + o, std::min(PIECE, r.second - o));
    std::atomic<size_t> next{0};
    parallel(NT, [&](int) {
      for (;;) {
        size_t i = next.fetch_add(1, std::memory_order_relaxed);
        if (i >= pieces.size()) break;
        madvise(pieces[i].first, pieces[i].second, MADV_DONTNEED);
      }
    });
    tick("released");
  }
#endif
  if (VERBOSE) {
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    fprintf(stderr, "rusage: user %.3f s, sys %.3f s, minflt %ld, majflt %ld, vcsw %ld, ivcsw %ld, maxrss %ld\n", ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6, ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6,
            ru.ru_minflt, ru.ru_majflt, ru.ru_nvcsw, ru.ru_nivcsw, ru.ru_maxrss);
  }
  fflush(stderr);
#ifdef PGO_GEN
  __gcov_dump();  // _exit skips the handler that would write the profile
#endif
  _exit(0);
}
