use either::Either;
use pollster::FutureExt;

use super::*;

/// ReadTransaction is a read-only transaction.
///
/// It is created by calling [`Tm::read`].
pub struct ReadTransaction<D: AsyncDatabase, S: AsyncSpawner, H> {
  pub(super) db: Tm<D, S, H>,
  pub(super) read_ts: u64,
}

impl<D, S, H> ReadTransaction<D, S, H>
where
  D: AsyncDatabase,
  S: AsyncSpawner,
{
  /// Looks for key and returns corresponding Item.
  pub async fn get<'a: 'b, 'b>(
    &'a self,
    key: &'b D::Key,
  ) -> Result<Option<Either<D::ItemRef<'a>, D::Item>>, D::Error> {
    self.db.inner.db.get(key, self.read_ts).await
  }

  /// Returns an iterator.
  pub async fn iter(&self, opts: IteratorOptions) -> D::Iterator<'_> {
    self
      .db
      .inner
      .db
      .iter(core::iter::empty(), self.read_ts, opts)
      .await
  }

  /// Returns an iterator over keys.
  pub async fn keys(&self, opts: KeysOptions) -> D::Keys<'_> {
    self
      .db
      .inner
      .db
      .keys(core::iter::empty(), self.read_ts, opts)
      .await
  }
}

impl<D, S, H> Drop for ReadTransaction<D, S, H>
where
  D: AsyncDatabase,
  S: AsyncSpawner,
{
  fn drop(&mut self) {
    self.db.inner.orc.done_read(self.read_ts).block_on();
  }
}
