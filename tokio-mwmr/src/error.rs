use super::{AsyncDatabase, AsyncPwm};

/// Error type for the transaction.
#[derive(thiserror::Error)]
pub enum TransactionError<P: AsyncPwm> {
  /// Returned if an update function is called on a read-only transaction.
  #[error("transaction is read-only")]
  ReadOnly,

  /// Returned when a transaction conflicts with another transaction. This can
  /// happen if the read rows had been updated concurrently by another transaction.
  #[error("transaction conflict, please retry")]
  Conflict,

  /// Returned if a previously discarded transaction is re-used.
  #[error("transaction has been discarded, please create a new one")]
  Discard,

  /// Returned if too many writes are fit into a single transaction.
  #[error("transaction is too large")]
  LargeTxn,

  /// Returned if the transaction manager error occurs.
  #[error("transaction manager error: {0}")]
  Manager(P::Error),
}

impl<P: AsyncPwm> core::fmt::Debug for TransactionError<P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::ReadOnly => write!(f, "ReadOnly"),
      Self::Conflict => write!(f, "Conflict"),
      Self::Discard => write!(f, "Discard"),
      Self::LargeTxn => write!(f, "LargeTxn"),
      Self::Manager(e) => write!(f, "Manager({:?})", e),
    }
  }
}

/// Error type for the [`TransactionDB`].
#[derive(thiserror::Error)]
pub enum Error<D: AsyncDatabase, P: AsyncPwm> {
  /// Returned if transaction related error occurs.
  #[error(transparent)]
  Transaction(#[from] TransactionError<P>),

  /// Returned if DB related error occurs.
  #[error(transparent)]
  DB(D::Error),
}

impl<D: AsyncDatabase, P: AsyncPwm> core::fmt::Debug for Error<D, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Transaction(e) => e.fmt(f),
      Self::DB(e) => e.fmt(f),
    }
  }
}

impl<D: AsyncDatabase, P: AsyncPwm> Error<D, P> {
  /// Create a new error from the database error.
  pub fn database(err: D::Error) -> Self {
    Self::DB(err)
  }

  /// Create a new error from the transaction error.
  pub fn transaction(err: TransactionError<P>) -> Self {
    Self::Transaction(err)
  }
}
