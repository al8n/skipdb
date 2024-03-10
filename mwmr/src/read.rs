use super::*;

/// ReadTransaction is a read-only transaction.
///
/// It is created by calling [`TransactionDB::read`].
pub struct ReadTransaction<D: Database, S> {
  pub(super) db: TransactionDB<D, S>,
  pub(super) read_ts: u64,
}

impl<D, S> ReadTransaction<D, S>
where
  D: Database,
{
  /// Looks for key and returns corresponding Item.
  pub fn get<'a: 'b, 'b>(
    &'a self,
    key: &'b D::Key,
  ) -> Result<Option<Either<D::ItemRef<'a>, D::Item>>, D::Error> {
    self.db.inner.db.get(key, self.read_ts)
  }

  /// Returns an iterator.
  pub fn iter(&self, opts: IteratorOptions) -> D::Iterator<'_> {
    self
      .db
      .inner
      .db
      .iter(core::iter::empty(), self.read_ts, opts)
  }

  /// Returns an iterator over keys.
  pub fn keys(&self, opts: KeysOptions) -> D::Keys<'_> {
    self
      .db
      .inner
      .db
      .keys(core::iter::empty(), self.read_ts, opts)
  }
}

impl<D, S> Drop for ReadTransaction<D, S>
where
  D: Database,
{
  fn drop(&mut self) {
    self.db.inner.orc.done_read(self.read_ts);
  }
}
