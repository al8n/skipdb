use super::*;

/// ReadTransaction is a read-only transaction.
///
/// It is created by calling [`TransactionManager::read`],
/// the read transaction will automatically notify the transaction manager when it
/// is dropped. So, the end user doesn't need to call any cleanup function, but must
/// hold this struct in their final read transaction implementation.
pub struct ReadTransaction<K, V, C, P> {
  pub(super) db: TransactionManager<K, V, C, P>,
  pub(super) read_ts: u64,
}

impl<K, V, C, P> ReadTransaction<K, V, C, P> {
  /// Returns the version of this read transaction.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.read_ts
  }
}

impl<K, V, C, P> Drop for ReadTransaction<K, V, C, P> {
  fn drop(&mut self) {
    self.db.inner.done_read(self.read_ts);
  }
}
