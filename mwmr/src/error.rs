use mwmr_core::sync::ConflictManager;

use super::PendingManager;

/// Error type for the transaction.
#[derive(thiserror::Error)]
pub enum TransactionError<C: ConflictManager, P: PendingManager> {
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
  PendingManager(P::Error),

  /// Returned if the conflict manager error occurs.
  #[error("conflict manager error: {0}")]
  ConflictManager(C::Error),
}

impl<C: ConflictManager, P: PendingManager> core::fmt::Debug for TransactionError<C, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::ReadOnly => write!(f, "ReadOnly"),
      Self::Conflict => write!(f, "Conflict"),
      Self::Discard => write!(f, "Discard"),
      Self::LargeTxn => write!(f, "LargeTxn"),
      Self::PendingManager(e) => write!(f, "PendingManager({:?})", e),
      Self::ConflictManager(e) => write!(f, "ConflictManager({:?})", e),
    }
  }
}

impl<C: ConflictManager, P: PendingManager> TransactionError<C, P> {
  /// Create a new error from the database error.
  #[inline]
  pub const fn conflict(err: C::Error) -> Self {
    Self::ConflictManager(err)
  }

  /// Create a new error from the transaction error.
  #[inline]
  pub const fn pending(err: P::Error) -> Self {
    Self::PendingManager(err)
  }
}

/// Error type for write transaction.
pub enum WriteTransactionError<C: ConflictManager, P: PendingManager, E: std::error::Error> {
  /// Returned if the transaction error occurs. 
  Transaction(TransactionError<C, P>),
  /// Returned if the write error occurs.
  Commit(E),
}

impl<C: ConflictManager, P: PendingManager, E: std::error::Error> core::fmt::Debug for WriteTransactionError<C, P, E> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Transaction(e) => write!(f, "Transaction({:?})", e),
      Self::Commit(e) => write!(f, "Commit({:?})", e),
    }
  }
}

impl<C: ConflictManager, P: PendingManager, E: std::error::Error> core::fmt::Display for WriteTransactionError<C, P, E> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Transaction(e) => write!(f, "transaction error: {e}"),
      Self::Commit(e) => write!(f, "commit error: {e}"),
    }
  }
}

impl<C: ConflictManager, P: PendingManager, E: std::error::Error> std::error::Error for WriteTransactionError<C, P, E> {}

impl<C: ConflictManager, P: PendingManager, E: std::error::Error> From<TransactionError<C, P>> for WriteTransactionError<C, P, E> {
  #[inline]
  fn from(err: TransactionError<C, P>) -> Self {
    Self::Transaction(err)
  }
}

impl<C: ConflictManager, P: PendingManager, E: std::error::Error> WriteTransactionError<C, P, E> {
  /// Create a new error from the transaction error.
  #[inline]
  pub const fn transaction(err: TransactionError<C, P>) -> Self {
    Self::Transaction(err)
  }

  /// Create a new error from the commit error.
  #[inline]
  pub const fn commit(err: E) -> Self {
    Self::Commit(err)
  }
}