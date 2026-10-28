//! The resampling generator of the measurement tests: one fully specified
//! generator, so a named seed fixes every resample of every tool that uses
//! it (`label_measure`, ADR-056; `season_measure`, #155).

/// SplitMix64 (Steele, Lea and Flood 2014): a fully specified generator,
/// so the seed fixes every resample.
pub struct SplitMix64(pub u64);

impl SplitMix64 {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` from the high half of a 128-bit product (bias
    /// below n / 2⁶⁴).
    pub fn below(&mut self, n: usize) -> usize {
        ((u128::from(self.next()) * n as u128) >> 64) as usize
    }
}

/// One circular block resample of `days` units: blocks of `block`
/// consecutive units, wrapping at the end, from uniform starts, until it
/// holds `days` of them; `add` sees every unit drawn, in order. The draws
/// depend on `rng`, `days` and `block` only. `days` and `block` are
/// positive.
pub fn resample(rng: &mut SplitMix64, days: usize, block: usize, mut add: impl FnMut(usize)) {
    let mut filled = 0;
    while filled < days {
        let start = rng.below(days);
        let take = block.min(days - filled);
        for k in 0..take {
            add((start + k) % days);
        }
        filled += take;
    }
}

/// A standard normal from two uniforms (Box and Muller 1958).
pub fn normal(rng: &mut SplitMix64) -> f64 {
    let uniform = |rng: &mut SplitMix64| ((rng.next() >> 11) as f64 + 0.5) / (1_u64 << 53) as f64;
    let (u, v) = (uniform(rng), uniform(rng));
    (-2.0 * u.ln()).sqrt() * (std::f64::consts::TAU * v).cos()
}
