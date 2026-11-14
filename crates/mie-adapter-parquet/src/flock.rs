//! The guard of every store lock (#220).
//!
//! Every lock of a store (the replication pass lock, the verification
//! lock, a source's writer lock, the copy-ledger lock, the adoption's probe
//! of a writer on `--from`) is an exclusive `flock` on a lock file, held
//! through a [`HeldLock`]. A `flock` belongs to the open file description,
//! not to the descriptor, and `flock(2)` keeps it until every descriptor of
//! that description is closed. A child process started by any thread of
//! the process holds a copy of every descriptor from its fork until its
//! exec (`O_CLOEXEC` closes them only at the exec). A lock released only by
//! closing its descriptor therefore stays held for that window, and the
//! next take, in this process or another, is refused as `Locked`. The `mie`
//! binary starts no process, but the test suites do, and under load the
//! window stretched far enough to fail tests that take a lock again right
//! after dropping it (#198, #220).
//!
//! [`HeldLock`] releases explicitly instead: its drop unlocks
//! (`flock(LOCK_UN)`, [`File::unlock`]), which frees the description's lock
//! whatever other descriptors of it exist. The workspace's `clippy.toml`
//! bans the direct [`File`] lock calls everywhere else, so no lock can
//! bypass this rule; the impl blocks here are the only ones allowed to make
//! them.
//!
//! The rule is also a constraint: code must never rely on a duplicate of a
//! lock descriptor keeping the lock after its guard dropped.

use std::fs::{File, TryLockError};
use std::io;

/// An exclusive `flock` on a lock file, unlocked explicitly when dropped
/// (see the module docs).
#[derive(Debug)]
pub struct HeldLock {
    file: File,
}

#[allow(clippy::disallowed_methods)]
impl HeldLock {
    /// Takes the exclusive lock on `file` without waiting. `file` may be
    /// opened read-only.
    ///
    /// # Errors
    ///
    /// [`TryLockError::WouldBlock`] when another holder has the lock;
    /// [`TryLockError::Error`] when it cannot be taken.
    pub fn try_take(file: File) -> Result<Self, TryLockError> {
        file.try_lock()?;
        Ok(Self { file })
    }

    /// Takes the exclusive lock on `file`, waiting for any holder.
    ///
    /// # Errors
    ///
    /// The I/O error when it cannot be taken.
    pub fn take(file: File) -> io::Result<Self> {
        file.lock()?;
        Ok(Self { file })
    }

    /// Releases the lock now and reports a failure, which the drop cannot.
    /// The drop after it unlocks again, which is a no-op.
    ///
    /// # Errors
    ///
    /// The I/O error of the unlock.
    pub fn release(&self) -> io::Result<()> {
        self.file.unlock()
    }

    /// A second descriptor of the held lock's open file description, as a
    /// child process started by another thread holds it between its fork
    /// and its exec.
    #[cfg(test)]
    pub(crate) fn duplicate(&self) -> File {
        self.file
            .try_clone()
            .expect("duplicate the lock descriptor")
    }
}

#[allow(clippy::disallowed_methods)]
impl Drop for HeldLock {
    fn drop(&mut self) {
        // A drop can neither return the error nor panic. If the unlock
        // fails, closing the descriptor still releases the lock once no
        // other descriptor of it is open, as before #220.
        let _ = self.file.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TempDir;
    use std::fs::OpenOptions;
    use std::path::Path;

    fn open(path: &Path) -> File {
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)
            .unwrap()
    }

    fn refused(result: Result<HeldLock, TryLockError>) -> bool {
        matches!(result, Err(TryLockError::WouldBlock))
    }

    #[test]
    fn release_then_drop_unlocks_without_error() {
        let dir = TempDir::new("flock-release");
        let path = dir.path().join(".lock");
        let held = HeldLock::try_take(open(&path)).unwrap();
        held.release().unwrap();
        // A second release is a no-op too.
        held.release().unwrap();
        let other = HeldLock::try_take(open(&path)).unwrap();
        // The released guard's drop unlocks its own description only.
        drop(held);
        assert!(refused(HeldLock::try_take(open(&path))));
        drop(other);
        HeldLock::try_take(open(&path)).unwrap();
    }

    #[test]
    fn a_read_only_descriptor_is_taken_and_released() {
        let dir = TempDir::new("flock-read-only");
        let path = dir.path().join(".lock");
        drop(open(&path));
        let held = HeldLock::try_take(File::open(&path).unwrap()).unwrap();
        assert!(refused(HeldLock::try_take(open(&path))));
        let child = held.duplicate();
        drop(held);
        HeldLock::try_take(open(&path)).unwrap();
        drop(child);
    }

    #[test]
    fn try_take_refuses_while_held_and_succeeds_after_drop() {
        let dir = TempDir::new("flock-try-take");
        let path = dir.path().join(".lock");
        let held = HeldLock::take(open(&path)).unwrap();
        drop(held.duplicate());
        assert!(refused(HeldLock::try_take(open(&path))));
        let child = held.duplicate();
        drop(held);
        let again = HeldLock::try_take(open(&path));
        assert!(again.is_ok(), "{again:?}");
        drop(child);
    }
}
