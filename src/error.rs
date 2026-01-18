#[derive(Debug)]
pub enum ErrorStatus {
    Permanent,  // Don't retry
    Temporary,  // Safe to retry
    Persistent, // Was retried, still failing
}
