// Generic (slow, fully general) pipeline. Used only when the fast path in
// main.cpp meets an input it cannot represent exactly.
// mmap inputs -> parallel parse -> dedup by event_id -> partition by person
// -> sessionize + attribute -> hand-written Parquet.
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <chrono>
#ifndef MAP_POPULATE
#define MAP_POPULATE 0
#endif

namespace {  // every name below is private to this file
using i64 = int64_t;
using u64 = uint64_t;
using u32 = uint32_t;
using u16 = uint16_t;
using u8 = uint8_t;
using std::string;
using std::string_view;
using std::vector;

static const i64 SEC = 1000, MIN_ = 60 * SEC, HOUR = 60 * MIN_, DAY = 24 * HOUR;
static int NT = 8;
static bool VERBOSE = false;
static auto T0 = std::chrono::steady_clock::now();
static void tick(const char* what) {
  if (!VERBOSE) return;
  double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - T0).count();
  fprintf(stderr, "[%8.1f ms] %s\n", ms, what);
}

// ---------------------------------------------------------------- parallel
template <class F>
static void parallel(int n, F&& f) {  // f(thread_index)
  vector<std::thread> ts;
  for (int i = 1; i < n; i++) ts.emplace_back([&, i] { f(i); });
  f(0);
  for (auto& t : ts) t.join();
}
template <class F>
static void parallel_for(size_t count, F&& f) {  // f(item, thread)
  std::atomic<size_t> next{0};
  parallel(NT, [&](int t) {
    for (;;) {
      size_t i = next.fetch_add(1, std::memory_order_relaxed);
      if (i >= count) break;
      f(i, t);
    }
  });
}

// ---------------------------------------------------------------- keys
// 16 bytes. Inline when len <= 15 (bytes 0..14, byte 15 = len), else
// a = pointer, b = len | 0xFF << 56. Long keys that must compare by (a,b)
// are canonicalized through intern_long.
struct Key {
  u64 a = 0, b = 0;
  bool is_long() const { return (b >> 56) == 0xFF; }
  u32 len() const { return is_long() ? (u32)(b & 0xFFFFFFFFu) : (u32)(b >> 56); }
  const char* ptr() const { return is_long() ? (const char*)a : (const char*)this; }
  string_view view() const { return string_view(ptr(), len()); }
};
static inline Key make_key(const char* p, u32 n) {
  Key k;
  if (n <= 15) {
    unsigned char buf[16] = {0};
    memcpy(buf, p, n);
    buf[15] = (unsigned char)n;
    memcpy(&k, buf, 16);
  } else {
    k.a = (u64)(uintptr_t)p;
    k.b = (u64)n | (0xFFull << 56);
  }
  return k;
}
static inline bool key_eq(const Key& x, const Key& y) {
  if (x.a == y.a && x.b == y.b) return true;
  if (x.is_long() && y.is_long()) return x.view() == y.view();
  return false;
}
static inline u64 mix(u64 h) {
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccdULL;
  h ^= h >> 33;
  h *= 0xc4ceb9fe1a85ec53ULL;
  h ^= h >> 33;
  return h;
}
static inline u64 key_hash(const Key& k) {
  if (!k.is_long()) return mix(k.a * 0x9E3779B97F4A7C15ULL ^ mix(k.b));
  u64 h = 0x12345678;
  string_view v = k.view();
  for (char c : v) h = (h ^ (unsigned char)c) * 0x100000001b3ULL;
  return mix(h ^ v.size());
}
static inline int key_cmp(const Key& x, const Key& y) {  // byte-wise
  string_view a = x.view(), b = y.view();
  int c = a.compare(b);
  return c < 0 ? -1 : (c > 0 ? 1 : 0);
}
static std::mutex g_intern_mu;
static std::unordered_map<string, const char*>* g_intern = new std::unordered_map<string, const char*>();
static Key intern_long(const Key& k) {
  if (!k.is_long()) return k;
  std::lock_guard<std::mutex> lk(g_intern_mu);
  string s(k.view());
  auto it = g_intern->find(s);
  if (it != g_intern->end()) return make_key(it->second, (u32)s.size());
  (*g_intern)[s] = k.ptr();
  return k;
}

// Open-addressing map Key -> u32. Single-writer during build, read-only after.
struct KeyMap {
  vector<Key> keys;
  vector<u32> vals;
  vector<u8> used;
  u64 mask = 0;
  size_t n = 0;
  void init(size_t cap) {
    size_t s = 16;
    while (s < cap * 2) s <<= 1;
    keys.assign(s, Key());
    vals.assign(s, 0);
    used.assign(s, 0);
    mask = s - 1;
    n = 0;
  }
  // returns pointer to value; inserted set if new
  u32* insert(const Key& k, u32 v, bool& inserted) {
    if ((n + 1) * 2 > keys.size()) grow();
    u64 i = key_hash(k) & mask;
    while (used[i]) {
      if (key_eq(keys[i], k)) {
        inserted = false;
        return &vals[i];
      }
      i = (i + 1) & mask;
    }
    used[i] = 1;
    keys[i] = k;
    vals[i] = v;
    n++;
    inserted = true;
    return &vals[i];
  }
  const u32* find(const Key& k) const {
    if (keys.empty()) return nullptr;
    u64 i = key_hash(k) & mask;
    while (used[i]) {
      if (key_eq(keys[i], k)) return &vals[i];
      i = (i + 1) & mask;
    }
    return nullptr;
  }
  void grow() {
    vector<Key> ok;
    vector<u32> ov;
    vector<u8> ou;
    ok.swap(keys);
    ov.swap(vals);
    ou.swap(used);
    init(std::max<size_t>(16, ok.size()));
    for (size_t i = 0; i < ok.size(); i++)
      if (ou[i]) {
        bool ins;
        insert(ok[i], ov[i], ins);
      }
  }
};

// ---------------------------------------------------------------- files
struct MFile {
  const char* p = nullptr;
  size_t n = 0;
};
static MFile map_file(const string& path) {
  MFile f;
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) return f;
  struct stat st;
  fstat(fd, &st);
  f.n = st.st_size;
  if (f.n > 0) {
    void* m = mmap(nullptr, f.n, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (m == MAP_FAILED) {
      f.n = 0;
    } else {
      f.p = (const char*)m;
      madvise(m, f.n, MADV_SEQUENTIAL);
    }
  }
  close(fd);
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
struct Chunk {
  const char* s;
  const char* e;
};
// Line-aligned chunks of all files in a folder, header lines removed.
static vector<Chunk> chunk_folder(const string& dir, vector<MFile>& keep, size_t target = 4 << 20) {
  vector<Chunk> out;
  vector<string> files = list_csv(dir);
  vector<MFile> maps(files.size());
  parallel_for(files.size(), [&](size_t i, int) { maps[i] = map_file(files[i]); });
  for (auto& f : maps) {
    if (!f.p) continue;
    keep.push_back(f);
    const char* s = f.p;
    const char* e = f.p + f.n;
    const char* nl = (const char*)memchr(s, '\n', e - s);
    if (!nl) continue;  // header only, no newline
    s = nl + 1;
    while (s < e) {
      const char* c = s + target;
      if (c >= e) {
        out.push_back({s, e});
        break;
      }
      const char* q = (const char*)memchr(c, '\n', e - c);
      if (!q) {
        out.push_back({s, e});
        break;
      }
      out.push_back({s, q + 1});
      s = q + 1;
    }
  }
  return out;
}

// ---------------------------------------------------------------- CSV
// Splits [s,e) (no newline) into exactly nf fields; returns false if malformed.
static inline bool split(const char* s, const char* e, int nf, const char** fp, u32* fl) {
  if (s == e) return false;
  bool anyq = memchr(s, '"', e - s) != nullptr;
  int k = 0;
  const char* p = s;
  for (;;) {
    const char* c = (const char*)memchr(p, ',', e - p);
    const char* fe = c ? c : e;
    if (k >= nf) return false;
    u32 len = (u32)(fe - p);
    if (anyq && len > 0 && (p[0] == '"' || memchr(p, '"', len))) {
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
template <class F>
static inline void for_lines(const Chunk& c, F&& f) {
  const char* s = c.s;
  while (s < c.e) {
    const char* nl = (const char*)memchr(s, '\n', c.e - s);
    const char* le = nl ? nl : c.e;
    f(s, le);
    s = le + 1;
  }
}

// ---------------------------------------------------------------- time
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
// Parses F1/F2/F3. Returns false when bad. Huge F3 values give TS_HUGE.
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
    off = (i64)(oh * 60 + om) * MIN_;
    if (sg == '-') off = -off;
  }
  int y = (p[0] - '0') * 1000 + (p[1] - '0') * 100 + (p[2] - '0') * 10 + (p[3] - '0');
  int mo = d2(p + 5), d = d2(p + 8), h = d2(p + 11), mi = d2(p + 14), s = d2(p + 17);
  if (mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || s > 59) return false;
  static const int dim[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int md = dim[mo];
  if (mo == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) md = 29;
  if (d > md) return false;
  out = (days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + s) * SEC - off;
  return true;
}
// price_cents for purchases: digits, 1..10,000,000
static inline bool parse_price(const char* p, u32 n, u32& out) {
  if (n == 0) return false;
  for (u32 i = 0; i < n; i++)
    if (!isd(p[i])) return false;
  u32 i = 0;
  while (i < n && p[i] == '0') i++;
  if (n - i > 8) return false;
  u32 v = 0;
  for (; i < n; i++) v = v * 10 + (p[i] - '0');
  if (v < 1 || v > 10000000) return false;
  out = v;
  return true;
}

// ---------------------------------------------------------------- records
struct EvRec {  // after parse, before dedup
  Key eid;
  Key user;
  i64 ingest;
  i64 ts;
  u32 price;
  u16 cat;
  u8 type;  // 0 view 1 cart 2 purchase
  u8 valid;
};
struct PRec {  // a surviving event, keyed by person
  Key name;
  i64 ts;
  u32 price;
  u16 cat;
  u8 purchase;
};
struct CRec {  // a valid click, keyed by person
  Key name;
  i64 ts;
  Key cid;
  u32 camp;
};
struct UDRow {
  Key name;
  int32_t day;
  i64 sessions, events, revenue;
  int32_t top;  // category rank, -1 null
};
struct CAgg {
  i64 sessions = 0, purch = 0, revenue = 0, assisted = 0;
};

// ---------------------------------------------------------------- parquet
struct TW {  // thrift compact writer
  vector<u8> b;
  vector<int> last{0};
  void byte(u8 x) { b.push_back(x); }
  void varint(u64 v) {
    while (v >= 0x80) {
      b.push_back((u8)(v | 0x80));
      v >>= 7;
    }
    b.push_back((u8)v);
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
    b.insert(b.end(), s.begin(), s.end());
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
  void list_begin_elem() { last.push_back(0); }  // struct element in list
  void list_end_elem() {
    byte(0);
    last.pop_back();
  }
};
enum PT { P_INT32 = 1, P_INT64 = 2, P_BYTES = 6 };
struct PCol {
  string name;
  int ptype;
  int conv = -1;  // 0 UTF8, 6 DATE
  int logical = 0;  // union field id: 1 STRING, 6 DATE
  bool optional = false;
  vector<u8> data;
  vector<u8> defs;  // one byte per row, 0/1
  i64 rows = 0;
  void put_i32(int32_t v) {
    data.insert(data.end(), (u8*)&v, (u8*)&v + 4);
    rows++;
  }
  void put_i64(i64 v) {
    data.insert(data.end(), (u8*)&v, (u8*)&v + 8);
    rows++;
  }
  void put_str(string_view s) {
    u32 n = (u32)s.size();
    data.insert(data.end(), (u8*)&n, (u8*)&n + 4);
    data.insert(data.end(), s.begin(), s.end());
    if (optional) defs.push_back(1);
    rows++;
  }
  void put_null() {
    defs.push_back(0);
    rows++;
  }
};
static void write_parquet(const string& path, vector<PCol>& cols, i64 nrows) {
  vector<u8> out;
  auto app = [&](const void* p, size_t n) { out.insert(out.end(), (const u8*)p, (const u8*)p + n); };
  app("PAR1", 4);
  struct Meta {
    i64 off, size;
  };
  vector<Meta> metas;
  for (auto& c : cols) {
    vector<u8> page;
    if (c.optional) {
      // RLE/bit-packed hybrid, bit width 1, one bit-packed run.
      vector<u8> lv;
      size_t groups = (c.defs.size() + 7) / 8;
      TW t;
      t.varint((groups << 1) | 1);
      lv = t.b;
      size_t base = lv.size();
      lv.resize(base + groups, 0);
      for (size_t i = 0; i < c.defs.size(); i++)
        if (c.defs[i]) lv[base + i / 8] |= (u8)(1 << (i % 8));
      u32 ln = (u32)lv.size();
      page.insert(page.end(), (u8*)&ln, (u8*)&ln + 4);
      page.insert(page.end(), lv.begin(), lv.end());
    }
    page.insert(page.end(), c.data.begin(), c.data.end());
    TW h;
    h.i32f(1, 0);  // DATA_PAGE
    h.i32f(2, (int32_t)page.size());
    h.i32f(3, (int32_t)page.size());
    h.begin_struct(5);
    h.i32f(1, (int32_t)nrows);
    h.i32f(2, 0);  // PLAIN
    h.i32f(3, 3);  // RLE
    h.i32f(4, 3);  // RLE
    h.end_struct();
    h.byte(0);
    i64 off = out.size();
    app(h.b.data(), h.b.size());
    app(page.data(), page.size());
    metas.push_back({off, (i64)(h.b.size() + page.size())});
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
    m.i64f(2, metas[i].off);
    m.begin_struct(3);
    m.i32f(1, c.ptype);
    m.list(2, 5, 2);
    m.varint(TW::zz(0));
    m.varint(TW::zz(3));
    m.list(3, 8, 1);
    m.varint(c.name.size());
    m.b.insert(m.b.end(), c.name.begin(), c.name.end());
    m.i32f(4, 0);
    m.i64f(5, nrows);
    m.i64f(6, metas[i].size);
    m.i64f(7, metas[i].size);
    m.i64f(9, metas[i].off);
    m.end_struct();
    m.list_end_elem();
    total += metas[i].size;
  }
  m.i64f(2, total);
  m.i64f(3, nrows);
  m.list_end_elem();
  m.strf(6, "sessions-cpp 2.4 (build 0d365338c9c2)");
  m.byte(0);
  app(m.b.data(), m.b.size());
  u32 ml = (u32)m.b.size();
  app(&ml, 4);
  app("PAR1", 4);
  FILE* f = fopen(path.c_str(), "wb");
  if (!f) {
    perror(path.c_str());
    exit(1);
  }
  fwrite(out.data(), 1, out.size(), f);
  fclose(f);
}

// ---------------------------------------------------------------- main
static string env(const char* k, const char* d) {
  const char* v = getenv(k);
  return v && *v ? string(v) : string(d);
}
static string read_all(const string& path) {
  FILE* f = fopen(path.c_str(), "rb");
  if (!f) return "";
  string s;
  char buf[65536];
  size_t n;
  while ((n = fread(buf, 1, sizeof buf, f)) > 0) s.append(buf, n);
  fclose(f);
  return s;
}
static i64 manifest_ts(const string& js, const char* key) {
  size_t k = js.find(string("\"") + key + "\"");
  if (k == string::npos) exit(2);
  size_t c = js.find(':', k);
  size_t q1 = js.find('"', c + 1);
  size_t q2 = js.find('"', q1 + 1);
  i64 v;
  if (!parse_ts(js.data() + q1 + 1, (u32)(q2 - q1 - 1), v)) exit(3);
  return v;
}

}  // namespace

int fallback_main(const std::string& data_in, const std::string& outdir_in) {
  string data = data_in, outdir = outdir_in;
  NT = atoi(env("NCPU", "0").c_str());
  if (NT <= 0) NT = (int)std::thread::hardware_concurrency();
  if (NT <= 0) NT = 8;
  VERBOSE = getenv("VERBOSE") != nullptr;

  string man = read_all(data + "/manifest.json");
  const i64 WS = manifest_ts(man, "window_start"), WE = manifest_ts(man, "window_end");
  const i64 CWS = WS - 7 * DAY;
  const i64 DAY0 = WS / DAY;  // WS > 0
  const int NDAYS = (int)((WE - 1) / DAY - DAY0 + 1);

  vector<MFile> keep;
  // ---- products and campaigns
  vector<string> cat_names;
  KeyMap prod;
  vector<u32> prod_cat_raw;
  {
    MFile f = map_file(data + "/products.csv");
    keep.push_back(f);
    std::unordered_map<string, u32> cats;
    prod.init(1 << 16);
    if (f.p) {
      const char* s = (const char*)memchr(f.p, '\n', f.n);
      Chunk c{s ? s + 1 : f.p + f.n, f.p + f.n};
      for_lines(c, [&](const char* a, const char* b) {
        const char* fp[2];
        u32 fl[2];
        if (!split(a, b, 2, fp, fl)) return;
        string cn(fp[1], fl[1]);
        auto it = cats.find(cn);
        u32 ci;
        if (it == cats.end()) {
          ci = (u32)cat_names.size();
          cats[cn] = ci;
          cat_names.push_back(cn);
        } else
          ci = it->second;
        bool ins;
        prod.insert(make_key(fp[0], fl[0]), ci, ins);
      });
    }
    if (!cats.count("unknown")) cat_names.push_back("unknown");
  }
  // rank categories byte-wise; product map stores rank
  const u32 NCAT = (u32)cat_names.size();
  u32 UNKNOWN_CAT;
  {
    vector<u32> order(NCAT);
    for (u32 i = 0; i < NCAT; i++) order[i] = i;
    std::sort(order.begin(), order.end(), [&](u32 x, u32 y) { return cat_names[x] < cat_names[y]; });
    vector<u32> rank(NCAT);
    vector<string> sorted(NCAT);
    for (u32 r = 0; r < NCAT; r++) rank[order[r]] = r, sorted[r] = cat_names[order[r]];
    for (size_t i = 0; i < prod.keys.size(); i++)
      if (prod.used[i]) prod.vals[i] = rank[prod.vals[i]];
    cat_names = sorted;
    UNKNOWN_CAT = (u32)(std::find(cat_names.begin(), cat_names.end(), "unknown") - cat_names.begin());
  }
  KeyMap camp;
  vector<string> camp_id, camp_chan;
  {
    MFile f = map_file(data + "/campaigns.csv");
    keep.push_back(f);
    camp.init(4096);
    if (f.p) {
      const char* s = (const char*)memchr(f.p, '\n', f.n);
      Chunk c{s ? s + 1 : f.p + f.n, f.p + f.n};
      for_lines(c, [&](const char* a, const char* b) {
        const char* fp[2];
        u32 fl[2];
        if (!split(a, b, 2, fp, fl)) return;
        bool ins;
        camp.insert(make_key(fp[0], fl[0]), (u32)camp_id.size(), ins);
        if (ins) camp_id.emplace_back(fp[0], fl[0]), camp_chan.emplace_back(fp[1], fl[1]);
      });
    }
  }
  const u32 NCAMP = (u32)camp_id.size();
  const u32 ORGANIC = NCAMP;
  tick("lookups");

  // ---- links -> union-find -> name per ID
  KeyMap idmap;
  vector<Key> name_of;  // by id
  {
    vector<Chunk> ch = chunk_folder(data + "/links", keep);
    vector<vector<std::pair<Key, Key>>> per(NT);
    parallel_for(ch.size(), [&](size_t i, int t) {
      for_lines(ch[i], [&](const char* a, const char* b) {
        const char* fp[2];
        u32 fl[2];
        if (!split(a, b, 2, fp, fl)) return;
        if (fl[0] == 0 || fl[1] == 0) return;
        per[t].emplace_back(make_key(fp[0], fl[0]), make_key(fp[1], fl[1]));
      });
    });
    tick("links parsed");
    size_t tot = 0;
    for (auto& v : per) tot += v.size();
    idmap.init(tot * 2 + 16);
    vector<Key> keys;
    vector<u32> parent;
    auto id = [&](const Key& k) {
      bool ins;
      u32* v = idmap.insert(k, (u32)keys.size(), ins);
      if (ins) keys.push_back(k), parent.push_back(*v);
      return *v;
    };
    std::function<u32(u32)> find = [&](u32 x) {
      while (parent[x] != x) {
        parent[x] = parent[parent[x]];
        x = parent[x];
      }
      return x;
    };
    for (auto& v : per)
      for (auto& pr : v) {
        u32 x = find(id(pr.first)), y = find(id(pr.second));
        if (x != y) parent[std::max(x, y)] = std::min(x, y);
      }
    vector<Key> best(keys.size());
    vector<u8> has(keys.size(), 0);
    for (u32 i = 0; i < keys.size(); i++) {
      u32 r = find(i);
      if (!has[r] || key_cmp(keys[i], best[r]) < 0) best[r] = keys[i], has[r] = 1;
    }
    name_of.resize(keys.size());
    for (u32 i = 0; i < keys.size(); i++) name_of[i] = intern_long(best[find(i)]);
  }
  tick("links done");
  auto name_for = [&](const Key& user) -> Key {
    const u32* v = idmap.find(user);
    if (v) return name_of[*v];
    return intern_long(user);
  };

  // ---- events: parse into eid partitions
  const int P = 256;
  auto part_of = [](u64 h) { return (int)(h >> 56); };
  vector<vector<vector<EvRec>>> evp(NT, vector<vector<EvRec>>(P));
  {
    vector<Chunk> ch = chunk_folder(data + "/events", keep);
    tick("events mapped");
    parallel_for(ch.size(), [&](size_t i, int t) {
      auto& out = evp[t];
      for_lines(ch[i], [&](const char* a, const char* b) {
        const char* fp[7];
        u32 fl[7];
        if (!split(a, b, 7, fp, fl)) return;
        if (fl[0] == 0) return;
        EvRec r;
        r.eid = make_key(fp[0], fl[0]);
        r.user = make_key(fp[1], fl[1]);
        if (!parse_ts(fp[3], fl[3], r.ingest)) r.ingest = TS_HUGE;  // never happens per rules
        r.price = 0;
        r.cat = 0;
        r.type = 0;
        r.ts = 0;
        bool ok = fl[1] > 0;
        ok = ok && parse_ts(fp[2], fl[2], r.ts) && r.ts >= WS && r.ts < WE;
        if (ok) {
          string_view ty(fp[4], fl[4]);
          if (ty == "view")
            r.type = 0;
          else if (ty == "cart")
            r.type = 1;
          else if (ty == "purchase")
            r.type = 2;
          else
            ok = false;
        }
        ok = ok && fl[5] > 0;
        if (ok && r.type == 2) ok = parse_price(fp[6], fl[6], r.price);
        if (ok && r.ingest - r.ts > DAY) ok = false;
        if (ok) {
          const u32* c = prod.find(make_key(fp[5], fl[5]));
          r.cat = (u16)(c ? *c : UNKNOWN_CAT);
        }
        r.valid = ok;
        out[part_of(key_hash(r.eid))].push_back(r);
      });
    });
  }
  tick("events parsed");

  // ---- dedup per eid partition -> person partitions
  vector<vector<vector<PRec>>> pp(NT, vector<vector<PRec>>(P));
  parallel_for(P, [&](size_t p, int t) {
    size_t n = 0;
    for (int s = 0; s < NT; s++) n += evp[s][p].size();
    size_t cap = 16;
    while (cap < n * 2) cap <<= 1;
    vector<const EvRec*> slot(cap, nullptr);
    u64 mask = cap - 1;
    for (int s = 0; s < NT; s++)
      for (const EvRec& r : evp[s][p]) {
        u64 i = key_hash(r.eid) & mask;
        for (;;) {
          const EvRec* q = slot[i];
          if (!q) {
            slot[i] = &r;
            break;
          }
          if (key_eq(q->eid, r.eid)) {
            if (r.ingest < q->ingest) slot[i] = &r;
            break;
          }
          i = (i + 1) & mask;
        }
      }
    auto& out = pp[t];
    for (const EvRec* q : slot) {
      if (!q || !q->valid) continue;
      PRec x;
      x.name = name_for(q->user);
      x.ts = q->ts;
      x.price = q->price;
      x.cat = q->cat;
      x.purchase = q->type == 2;
      out[part_of(key_hash(x.name))].push_back(x);
    }
  });
  tick("dedup done");
  evp.clear();
  evp.shrink_to_fit();
  tick("evp freed");

  // ---- clicks
  vector<vector<vector<CRec>>> cp(NT, vector<vector<CRec>>(P));
  {
    vector<Chunk> ch = chunk_folder(data + "/clicks", keep);
    parallel_for(ch.size(), [&](size_t i, int t) {
      auto& out = cp[t];
      for_lines(ch[i], [&](const char* a, const char* b) {
        const char* fp[4];
        u32 fl[4];
        if (!split(a, b, 4, fp, fl)) return;
        if (fl[0] == 0 || fl[1] == 0) return;
        CRec r;
        if (!parse_ts(fp[3], fl[3], r.ts) || r.ts < CWS || r.ts >= WE) return;
        const u32* c = camp.find(make_key(fp[2], fl[2]));
        if (!c) return;
        r.camp = *c;
        r.cid = make_key(fp[0], fl[0]);
        r.name = name_for(make_key(fp[1], fl[1]));
        out[part_of(key_hash(r.name))].push_back(r);
      });
    });
  }
  tick("clicks parsed");

  // ---- per person partition: sessionize, attribute
  vector<vector<UDRow>> ud(NT);
  vector<vector<CAgg>> ca(NT, vector<CAgg>((size_t)(NCAMP + 1) * NDAYS));
  parallel_for(P, [&](size_t p, int t) {
    vector<PRec> ev;
    vector<CRec> cl;
    for (int s = 0; s < NT; s++) {
      ev.insert(ev.end(), pp[s][p].begin(), pp[s][p].end());
      cl.insert(cl.end(), cp[s][p].begin(), cp[s][p].end());
    }
    std::sort(ev.begin(), ev.end(), [](const PRec& x, const PRec& y) {
      if (x.name.a != y.name.a) return x.name.a < y.name.a;
      if (x.name.b != y.name.b) return x.name.b < y.name.b;
      return x.ts < y.ts;
    });
    std::sort(cl.begin(), cl.end(), [](const CRec& x, const CRec& y) {
      if (x.name.a != y.name.a) return x.name.a < y.name.a;
      if (x.name.b != y.name.b) return x.name.b < y.name.b;
      if (x.ts != y.ts) return x.ts < y.ts;
      return key_cmp(x.cid, y.cid) < 0;
    });
    auto lt = [](const Key& x, const Key& y) { return x.a != y.a ? x.a < y.a : x.b < y.b; };
    auto& rows = ud[t];
    auto& agg = ca[t];
    vector<i64> catrev(NCAT, 0);
    vector<u32> stamp(NCAMP + 1, 0);
    u32 stampv = 0;
    size_t ci = 0;
    size_t i = 0;
    while (i < ev.size()) {
      Key name = ev[i].name;
      size_t j = i;
      while (j < ev.size() && ev[j].name.a == name.a && ev[j].name.b == name.b) j++;
      while (ci < cl.size() && lt(cl[ci].name, name)) ci++;
      size_t cj = ci;
      while (cj < cl.size() && cl[cj].name.a == name.a && cl[cj].name.b == name.b) cj++;
      // person's events [i,j), clicks [ci,cj)
      size_t lo = ci, hi = ci;
      int cur_day = -1;
      UDRow row{};
      auto flush = [&]() {
        if (cur_day < 0) return;
        row.top = -1;
        if (row.revenue > 0) {
          i64 best = -1;
          for (u32 c = 0; c < NCAT; c++)
            if (catrev[c] > best) best = catrev[c], row.top = (int32_t)c;
          // best>0 guaranteed
        }
        std::fill(catrev.begin(), catrev.end(), 0);
        rows.push_back(row);
      };
      size_t k = i;
      while (k < j) {
        i64 start = ev[k].ts;
        size_t m = k + 1;
        while (m < j && ev[m].ts - ev[m - 1].ts <= 30 * MIN_) m++;
        // session [k,m)
        int day = (int)(start / DAY - DAY0);
        if (day != cur_day) {
          flush();
          cur_day = day;
          row = UDRow{};
          row.name = name;
          row.day = (int32_t)(start / DAY);
        }
        i64 rev = 0;
        bool purch = false;
        for (size_t q = k; q < m; q++)
          if (ev[q].purchase) {
            rev += ev[q].price;
            purch = true;
            catrev[ev[q].cat] += ev[q].price;
          }
        row.sessions++;
        row.events += (i64)(m - k);
        row.revenue += rev;
        // attribution
        while (hi < cj && cl[hi].ts <= start) hi++;
        while (lo < hi && cl[lo].ts < start - 7 * DAY) lo++;
        u32 credit = ORGANIC;
        if (hi > lo) {
          size_t b = hi - 1;
          i64 mt = cl[b].ts;
          while (b > lo && cl[b - 1].ts == mt) b--;
          credit = cl[b].camp;
          stampv++;
          for (size_t q = lo; q < hi; q++) {
            u32 c = cl[q].camp;
            if (stamp[c] != stampv) {
              stamp[c] = stampv;
              agg[(size_t)c * NDAYS + day].assisted++;
            }
          }
        }
        CAgg& g = agg[(size_t)credit * NDAYS + day];
        g.sessions++;
        g.purch += purch;
        g.revenue += rev;
        k = m;
      }
      flush();
      i = j;
      ci = cj;
    }
  });
  tick("sessions done");

  // ---- outputs
  string udd = outdir + "/user_daily", cdd = outdir + "/campaign_daily";
  mkdir(outdir.c_str(), 0777);
  mkdir(udd.c_str(), 0777);
  mkdir(cdd.c_str(), 0777);
  std::thread cw([&] {
    vector<CAgg> tot((size_t)(NCAMP + 1) * NDAYS);
    for (int t = 0; t < NT; t++)
      for (size_t i = 0; i < tot.size(); i++) {
        tot[i].sessions += ca[t][i].sessions;
        tot[i].purch += ca[t][i].purch;
        tot[i].revenue += ca[t][i].revenue;
        tot[i].assisted += ca[t][i].assisted;
      }
    vector<PCol> cols(7);
    cols[0] = {"campaign_id", P_BYTES, 0, 1};
    cols[1] = {"channel", P_BYTES, 0, 1};
    cols[2] = {"day", P_INT32, 6, 6};
    cols[3] = {"sessions", P_INT64};
    cols[4] = {"purchasing_sessions", P_INT64};
    cols[5] = {"revenue_cents", P_INT64};
    cols[6] = {"assisted_sessions", P_INT64};
    i64 n = 0;
    for (u32 c = 0; c <= NCAMP; c++)
      for (int d = 0; d < NDAYS; d++) {
        const CAgg& g = tot[(size_t)c * NDAYS + d];
        if (!g.sessions && !g.assisted) continue;
        cols[0].put_str(c == ORGANIC ? string_view("organic") : string_view(camp_id[c]));
        cols[1].put_str(c == ORGANIC ? string_view("organic") : string_view(camp_chan[c]));
        cols[2].put_i32((int32_t)(DAY0 + d));
        cols[3].put_i64(g.sessions);
        cols[4].put_i64(g.purch);
        cols[5].put_i64(g.revenue);
        cols[6].put_i64(g.assisted);
        n++;
      }
    write_parquet(cdd + "/part-0.parquet", cols, n);
  });
  parallel(NT, [&](int t) {
    vector<PCol> cols(6);
    cols[0] = {"user_id", P_BYTES, 0, 1};
    cols[1] = {"day", P_INT32, 6, 6};
    cols[2] = {"sessions", P_INT64};
    cols[3] = {"events", P_INT64};
    cols[4] = {"revenue_cents", P_INT64};
    cols[5] = {"top_category", P_BYTES, 0, 1, true};
    for (auto& r : ud[t]) {
      cols[0].put_str(r.name.view());
      cols[1].put_i32(r.day);
      cols[2].put_i64(r.sessions);
      cols[3].put_i64(r.events);
      cols[4].put_i64(r.revenue);
      if (r.top < 0)
        cols[5].put_null();
      else
        cols[5].put_str(cat_names[r.top]);
    }
    write_parquet(udd + "/part-" + std::to_string(t) + ".parquet", cols, (i64)ud[t].size());
  });
  cw.join();
  tick("written");
  fflush(stderr);
  return 0;
}
