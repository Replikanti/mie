mie-experiment 1
# An experiment spec (spec text v1, ADR-040). Validate it with
#   mie experiment validate crates/mie-cli/experiment.example.spec
# which prints the experiment id and the canonical text: keys in canonical
# order, decimals with 8 places, no comments. The id is the fingerprint of
# that text, so any change to a field is a new experiment.

# The hypothesis this experiment tests; experiments of one hypothesis are
# listed together.
hypothesis vah.failed_auction.short

# Sample period: [start, end) in UTC epoch milliseconds
# (2025-10-01 to 2026-10-01).
sample 1759276800000 1790812800000

# The dataset version of the sample, as `mie replay` prints it for the same
# window and source. A run refuses any other raw data.
data 0000000000000000000000000000000000000000000000000000000000000000

# The feature set version and its features, closed over their upstream
# features (profile.volume.utc_day@1 builds on bars.time.1m@1). A wrong
# version is reported with the right one.
features 4a14739bcf46afb3 bars.time.1h@1,bars.time.1m@1,profile.volume.utc_day@1,volatility.atr.1h@1,volatility.regime.1h@1

# Filters: one line per filter, or `none` for explicitly unfiltered.
state-filter none
regime-filter volatility.regime.1h@1 HIGH,EXTREME

# Rules are versioned references with typed parameters
# (int, bool, text, price, qty, rate, feature).
location location.at_level@1 level=text:vah tolerance=rate:0.001
trigger trigger.failed_auction@1
entry entry.next_trade@1
invalidation invalidation.beyond_extreme@1
target target.level@1 level=text:poc

# Cost and latency assumptions are required.
fees maker=0.0002 taker=0.0005
slippage slippage.fixed@1 rate=rate:0.0001
funding funding.recorded@1
latency 250
