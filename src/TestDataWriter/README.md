# DataWriter cumulative snapshot regression tests

This fork adds two opt-in `writer` settings to `WtDataStorage`:

```yaml
reset_cumulative_on_start: true
cumulative_gap_seconds: 60
```

Both default to disabled (`false` / `0`) to preserve existing upstream behavior.
They apply only to cumulative quotes (`procFlag == 1`), including `ParserCTP`.
Preprocessed incremental quotes are unchanged.

With `reset_cumulative_on_start` enabled, a first snapshot received after the
session's first trading minute establishes the cumulative volume, turnover and
open-interest baseline without assigning prior activity to that minute. Opening
auction / first trading-minute cumulative values are retained. Session templates
must correctly describe the trading day, including any preceding night session.

For a persisted same-day cache, a restart within the same calendar minute retains
the exact cumulative difference. A restart across a minute boundary with elapsed
trading time resets the baseline and logs the unallocated difference. Optionally,
`cumulative_gap_seconds` also resets the baseline after a longer observed quote
gap in the running process. Scheduled session breaks are excluded from elapsed
trading time so ordinary lunch breaks retain the reopening increment.

This is a conservative data-quality policy: an observed gap can also occur in an
illiquid contract without a network failure. Set `cumulative_gap_seconds: 0` to
disable the running-process gap policy while retaining startup handling.
The excluded difference is recorded as `Cumulative recovery gap` in the log.
Zero incremental fields in the recovery snapshot mean **unmeasured**, not a
verified zero-trade interval. Retain the warning with the collected data and
exclude/mark affected partial minutes in analysis. The DMB/DSB format is unchanged;
no per-bar quality flag is embedded by this fix.

The first observed minute may still have incomplete OHLC. Neither this fix nor
the tests reconstruct missed quotes or change existing historical files.

## Run on Linux

Configure the root `src/` CMake project, then build `TestDataWriter`. The target
links the actual `WtDataStorage` shared library. Run it with a scratch directory
that does not already exist:

```bash
cmake --build <build-dir> --target TestDataWriter --parallel 2
<build-dir>/build_x64/Debug/bin/TestDataWriter/TestDataWriter /tmp/new-writer-test-data
```

The tests cover legacy behavior, midday initialization, volume/turnover/interest
increments, day and night openings, auction, persisted-cache restart, running quote
gaps, option disabling, trading-day changes, midnight, preprocessed quotes and
scheduled lunch breaks. Every writer runs in an isolated child process and uses
only generated quotes and scratch data; no CTP credentials or network are used.
The test children deliberately use `_Exit` to avoid the existing upstream thread
teardown issue. Passing these tests does not certify graceful shutdown.
