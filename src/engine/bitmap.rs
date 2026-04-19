use crate::storage::NullBitmap;

/// Packed bit-vector: bit set = row selected.
/// Uses u64 words for wide SIMD-friendly operations.
#[derive(Clone)]
pub(super) struct SelectionBitmap {
    words: Vec<u64>,
    len: usize,
    count: usize,
}

impl SelectionBitmap {
    /// All rows selected.
    pub fn all(len: usize) -> Self {
        let word_count = len.div_ceil(64);
        let mut words = vec![u64::MAX; word_count];
        // Clear unused trailing bits in the last word.
        let remainder = len % 64;
        if remainder > 0 && !words.is_empty() {
            *words.last_mut().unwrap() = (1u64 << remainder) - 1;
        }
        Self {
            words,
            len,
            count: len,
        }
    }

    /// No rows selected.
    pub fn none(len: usize) -> Self {
        let word_count = len.div_ceil(64);
        Self {
            words: vec![0u64; word_count],
            len,
            count: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn count(&self) -> usize {
        self.count
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Intersect in-place: self &= other.
    pub fn intersect(&mut self, other: &SelectionBitmap) {
        debug_assert_eq!(self.len, other.len);
        for (a, b) in self.words.iter_mut().zip(other.words.iter()) {
            *a &= *b;
        }
        self.recount();
    }

    pub fn union(&mut self, other: &SelectionBitmap) {
        debug_assert_eq!(self.len, other.len);
        for (a, b) in self.words.iter_mut().zip(other.words.iter()) {
            *a |= *b;
        }
        self.clear_unused_bits();
        self.recount();
    }

    pub fn clear(&mut self) {
        self.words.fill(0);
        self.count = 0;
    }

    /// Clear bits where the null bitmap indicates null (bit clear in NullBitmap = null).
    /// After this, only non-null selected rows remain.
    pub fn mask_non_null(&mut self, nulls: &NullBitmap) {
        for (word, mask) in self.words.iter_mut().zip(nulls.words()) {
            *word &= *mask;
        }
        self.recount();
    }

    /// Clear bits where the null bitmap indicates null (bit clear in NullBitmap = null),
    /// without refreshing the cached popcount. Call `recount()` after the final mask.
    pub fn mask_non_null_unchecked(&mut self, nulls: &NullBitmap) {
        for (word, mask) in self.words.iter_mut().zip(nulls.words()) {
            *word &= *mask;
        }
    }

    /// Keep only bits where the null bitmap indicates null (bit clear in NullBitmap = null).
    pub fn mask_null(&mut self, nulls: &NullBitmap) {
        self.mask_null_unchecked(nulls);
        self.recount();
    }

    /// Keep only bits where the null bitmap indicates null (bit clear in NullBitmap = null),
    /// without refreshing the cached popcount. Call `recount()` after the final mask.
    pub fn mask_null_unchecked(&mut self, nulls: &NullBitmap) {
        for (word, mask) in self.words.iter_mut().zip(nulls.words()) {
            *word &= !*mask;
        }
        self.clear_unused_bits();
    }

    pub fn retain_from_predicate_unchecked<F>(&mut self, mut pred: F)
    where
        F: FnMut(usize) -> bool,
    {
        for (word_idx, word) in self.words.iter_mut().enumerate() {
            let current = *word;
            if current == 0 {
                continue;
            }

            let base = word_idx * 64;
            let mut remaining = current;
            let mut retained = 0u64;
            while remaining != 0 {
                let tz = remaining.trailing_zeros() as usize;
                let mask = 1u64 << tz;
                if pred(base + tz) {
                    retained |= mask;
                }
                remaining &= remaining - 1;
            }
            *word = retained;
        }
    }

    /// Recompute cached popcount from words.
    pub fn recount(&mut self) {
        self.count = self.words.iter().map(|w| w.count_ones() as usize).sum();
    }

    /// Iterate over indices of set bits.
    pub fn iter_set(&self) -> BitmapIter<'_> {
        BitmapIter {
            words: &self.words,
            word_idx: 0,
            current: if self.words.is_empty() {
                0
            } else {
                self.words[0]
            },
            base: 0,
            len: self.len,
        }
    }

    fn clear_unused_bits(&mut self) {
        let remainder = self.len % 64;
        if remainder > 0 && !self.words.is_empty() {
            *self.words.last_mut().unwrap() &= (1u64 << remainder) - 1;
        }
    }
}

pub(super) struct BitmapIter<'a> {
    words: &'a [u64],
    word_idx: usize,
    current: u64,
    base: usize,
    len: usize,
}

impl Iterator for BitmapIter<'_> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        loop {
            if self.current != 0 {
                let tz = self.current.trailing_zeros() as usize;
                // Clear the lowest set bit.
                self.current &= self.current - 1;
                let idx = self.base + tz;
                if idx < self.len {
                    return Some(idx);
                }
                return None;
            }
            self.word_idx += 1;
            if self.word_idx >= self.words.len() {
                return None;
            }
            self.base = self.word_idx * 64;
            self.current = self.words[self.word_idx];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_and_none() {
        let all = SelectionBitmap::all(100);
        assert_eq!(all.count(), 100);
        assert_eq!(all.len(), 100);
        assert!(!all.is_empty());
        assert_eq!(all.iter_set().count(), 100);

        let none = SelectionBitmap::none(100);
        assert_eq!(none.count(), 0);
        assert!(none.is_empty());
        assert_eq!(none.iter_set().count(), 0);
    }

    #[test]
    fn all_iter_is_contiguous() {
        let all = SelectionBitmap::all(200);
        let indices: Vec<usize> = all.iter_set().collect();
        assert_eq!(indices.len(), 200);
        assert_eq!(indices[0], 0);
        assert_eq!(indices[199], 199);
    }

    #[test]
    fn intersect_works() {
        let mut a = SelectionBitmap::all(128);
        let mut b = SelectionBitmap::none(128);
        // Set even bits in b.
        for i in (0..128).step_by(2) {
            b.words[i / 64] |= 1u64 << (i % 64);
        }
        b.recount();
        assert_eq!(b.count(), 64);

        a.intersect(&b);
        assert_eq!(a.count(), 64);
        let indices: Vec<usize> = a.iter_set().collect();
        assert!(indices.iter().all(|i| i % 2 == 0));
    }

    #[test]
    fn mask_non_null() {
        // 8 rows, nulls at positions 2 and 5.
        let present = [true, true, false, true, true, false, true, true];
        let nulls = NullBitmap::from_bools(&present);

        let mut bitmap = SelectionBitmap::all(8);
        bitmap.mask_non_null(&nulls);
        assert_eq!(bitmap.count(), 6);
        let indices: Vec<usize> = bitmap.iter_set().collect();
        assert_eq!(indices, vec![0, 1, 3, 4, 6, 7]);
    }

    #[test]
    fn mask_null() {
        let present = [true, true, false, true, true, false, true, true];
        let nulls = NullBitmap::from_bools(&present);

        let mut bitmap = SelectionBitmap::all(8);
        bitmap.mask_null(&nulls);
        assert_eq!(bitmap.count(), 2);
        let indices: Vec<usize> = bitmap.iter_set().collect();
        assert_eq!(indices, vec![2, 5]);
    }

    #[test]
    fn non_aligned_lengths() {
        let bm = SelectionBitmap::all(65);
        assert_eq!(bm.count(), 65);
        assert_eq!(bm.iter_set().count(), 65);
        let last: usize = bm.iter_set().last().unwrap();
        assert_eq!(last, 64);

        let bm = SelectionBitmap::all(1);
        assert_eq!(bm.count(), 1);
        assert_eq!(bm.iter_set().collect::<Vec<_>>(), vec![0]);
    }

    #[test]
    fn mask_non_null_handles_non_aligned_lengths() {
        let present = (0..65).map(|index| index % 3 != 0).collect::<Vec<_>>();
        let nulls = NullBitmap::from_bools(&present);

        let mut bitmap = SelectionBitmap::all(65);
        bitmap.mask_non_null(&nulls);

        let expected = present.iter().filter(|&&is_present| is_present).count();
        assert_eq!(bitmap.count(), expected);
        assert!(bitmap.iter_set().all(|index| present[index]));
    }
}
