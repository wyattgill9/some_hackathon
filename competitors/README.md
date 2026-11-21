# Competitor reference code (read-only, not ours)

Shallow snapshots of five public Shopping Sessions entries, stripped to source, build config, Dockerfile and perf notes.
Each folder's `SOURCE.txt` has the repo URL and the commit. Harness copies, fixtures, rules.md, data, test/bench scripts,
reference implementations, and their CLAUDE.md/AGENTS.md agent files were removed.

| Folder | Lang | Board evidence | Start with |
|---|---|---|---|
| Dremix10_shopping-sessions-fast | C++ | origin of deltaparticle's 0.66 s code; 3 commits newer than that | src/main.cpp |
| deltaparticle_Databricks-Buildathon | C++ (shipped) + Rust (reference) | "scored 0.66" (Dremix10 @ 954c08d) | cpp/main.cpp, README.md, competitors-bench.yml (GH Actions job that benchmarks other repos' images) |
| Vibhor7-7_shopping-sessions | Rust | team VibDiv, #13, 0.78 s | SUBMISSIONS.md, BENCH.md, src/ |
| NhanHu_Test | Rust | unscored; commit claims under 0.55 s locally | README.md (design summary), src/ |
| cherylaurelia_builders-hackathon | Rust | unscored; ~0.39-0.46 s local, fastest public build nimaibhat measured | src/main.rs (single file) |

Do not copy code into our entry without checking the reuse policy with the organizers (deltaparticle claims an "open-reuse policy"; our rules.md doesn't mention one).
