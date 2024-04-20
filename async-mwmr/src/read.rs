use super::*;

/// AsyncRtm is a read-only transaction manager.
///
/// It is created by calling [`AsyncTm::read`],
/// the read transaction will automatically notify the transaction manager when it
/// is dropped. So, the end user doesn't need to call any cleanup function, but must
/// hold this struct in their final read transaction implementation.
pub struct AsyncRtm<K, V, C, P, S> {
  pub(super) db: AsyncTm<K, V, C, P, S>,
  pub(super) read_ts: u64,
}

impl<K, V, C, P, S> AsyncRtm<K, V, C, P, S> {
  /// Returns the version of this read transaction.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.read_ts
  }
}

impl<K, V, C, P, S> Drop for AsyncRtm<K, V, C, P, S> {
  fn drop(&mut self) {
    self.db.inner.done_read_blocking(self.read_ts);
  }
}
