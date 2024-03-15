use mwmr_core::sync::Cm;

use super::Pwm;

/// Error type for the transaction.
#[derive(thiserror::Error)]
pub enum TransactionError<C: Cm, P: Pwm> {
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
  Pwm(P::Error),

  /// Returned if the conflict manager error occurs.
  #[error("conflict manager error: {0}")]
  Cm(C::Error),
}

impl<C: Cm, P: Pwm> core::fmt::Debug for TransactionError<C, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::ReadOnly => write!(f, "ReadOnly"),
      Self::Conflict => write!(f, "Conflict"),
      Self::Discard => write!(f, "Discard"),
      Self::LargeTxn => write!(f, "LargeTxn"),
      Self::Pwm(e) => write!(f, "Pwm({:?})", e),
      Self::Cm(e) => write!(f, "Cm({:?})", e),
    }
  }
}

impl<C: Cm, P: Pwm> TransactionError<C, P> {
  /// Create a new error from the database error.
  #[inline]
  pub const fn conflict(err: C::Error) -> Self {
    Self::Cm(err)
  }

  /// Create a new error from the transaction error.
  #[inline]
  pub const fn pending(err: P::Error) -> Self {
    Self::Pwm(err)
  }
}

/// Error type for write transaction.
pub enum WtmError<C: Cm, P: Pwm, E: std::error::Error> {
  /// Returned if the transaction error occurs.
  Transaction(TransactionError<C, P>),
  /// Returned if the write error occurs.
  Commit(E),
}

impl<C: Cm, P: Pwm, E: std::error::Error> core::fmt::Debug for WtmError<C, P, E> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Transaction(e) => write!(f, "Transaction({:?})", e),
      Self::Commit(e) => write!(f, "Commit({:?})", e),
    }
  }
}

impl<C: Cm, P: Pwm, E: std::error::Error> core::fmt::Display for WtmError<C, P, E> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Transaction(e) => write!(f, "transaction error: {e}"),
      Self::Commit(e) => write!(f, "commit error: {e}"),
    }
  }
}

impl<C: Cm, P: Pwm, E: std::error::Error> std::error::Error for WtmError<C, P, E> {}

impl<C: Cm, P: Pwm, E: std::error::Error> From<TransactionError<C, P>> for WtmError<C, P, E> {
  #[inline]
  fn from(err: TransactionError<C, P>) -> Self {
    Self::Transaction(err)
  }
}

impl<C: Cm, P: Pwm, E: std::error::Error> WtmError<C, P, E> {
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
