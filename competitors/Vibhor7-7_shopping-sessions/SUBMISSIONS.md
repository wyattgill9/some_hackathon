# SUBMISSIONS.md: every submission on the track page

- Track page: https://www.joinbuilders.org/buildathon/tracks/shopping-sessions
- Repository to submit: https://github.com/Vibhor7-7/shopping-sessions (team VibDiv)
- **The last submitted version is the last row of the table below.**

## How the page works (as seen on 2026-11-21)

- The form has two fields, "Public GitHub repository" and "Commit (optional)", and a Submit button. Above it the page says how many of the 2 submissions an hour are left and when the next one frees up.
- "Your team's submissions" lists each one, newest first: its ID, its state, its score, when, and `repository @ commit` (12 characters of the hash). A submission that fails the dev check shows its differing rows and its log there.
- Every submission except the entered one has a "Pick as our final entry" button. **Never press it.** The best score is entered in the final without anyone choosing.
- Scores of two machines can differ by up to about 20%, the page says; runs under 20 s are timed 3 times and the median counts.
- Submissions close Sunday 2026-11-22 at 11:00 AM EST.

## Submissions

Rows up to `fac219f` were read off the page on 2026-11-21; the user made them by hand.

| When (EST) | ID | Commit | Result | Score | Missing / extra / differing rows | Note |
| --- | --- | --- | --- | --- | --- | --- |
| Fri 2026-11-20, 8:49 PM | BB-6F32C523 | `d698c87b14060adeeb2940552dab479a0ad85ce9` | Scored | 1.05 s | none | |
| Fri 9:35 PM | BB-11C83E8B | `8cb3e8e0a0360a83ac6c273ca0ce859ad80cea4f` | Scored | 0.99 s | none | |
| Fri 9:59 PM | BB-38921F8B | `6bbe143db79edca466dc7b773980ec45b2ac7e0e` | Scored | 0.98 s | none | |
| Fri 10:35 PM | BB-18D24A7C | `7f9bffe36278e04662689cb0a596e03f0cff1a74` | Scored | 0.96 s | none | |
| Fri 11:10 PM | BB-2AF57432 | `c24627f787beead6f4e68dd5056c64dbd9beedb7` | Scored | 0.97 s | none | |
| Fri 11:49 PM | BB-9603EAD8 | `c6b4e3e1f74c2fd49ad1a9667d832563a5188de1` | Scored | 0.82 s | none | |
| Sat 2026-11-21, 12:26 AM | BB-5C420AD1 | `7a5ddaf05a8aa81bd381832f3fff484c5ae6622a` | Scored | 0.80 s | none | |
| Sat 12:53 AM | BB-DEE36DC7 | `fac219fce1a16f657f26dacc27a532c5fed06d8b` | Scored | **0.78 s** | none | Entered in the final as the best score |

## Not submitted

- The probe (branch `probe`, `7b0a3c33ed0446e0aae34f45f182a122f5199e0d` when this was written). It fails the dev check on purpose, so it never meets the conditions for an unattended submission; spending a slot on it is the user's call.
