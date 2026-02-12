/// Utilities for gRPC calls with timeout and retry support
use std::time::Duration;
use tokio::time;

/// Configuration for retry behavior
pub struct RetryConfig {
    pub max_attempts: u32,
    pub timeout_per_attempt: Duration,
    pub delay_between_attempts: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            timeout_per_attempt: Duration::from_secs(10),
            delay_between_attempts: Duration::from_secs(2),
        }
    }
}

/// Execute an async operation with timeout and retry logic.
///
/// # Arguments
/// * `operation_name` - Human-readable name for logging
/// * `operation` - Async function to execute (should return Result)
/// * `config` - Retry configuration
///
/// # Returns
/// The result of the operation if successful within max_attempts, or the last error
pub async fn call_with_retry<F, Fut, T, E>(
    operation_name: &str,
    mut operation: F,
    config: &RetryConfig,
) -> Result<T, CallWithRetryError<E>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut last_error = None;

    for attempt in 1..=config.max_attempts {
        tracing::debug!(
            "{}: attempt {}/{}",
            operation_name,
            attempt,
            config.max_attempts
        );

        match time::timeout(config.timeout_per_attempt, operation()).await {
            Ok(Ok(result)) => {
                if attempt > 1 {
                    tracing::info!(
                        "{}: succeeded on attempt {}/{}",
                        operation_name,
                        attempt,
                        config.max_attempts
                    );
                }
                return Ok(result);
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    "{}: attempt {}/{} failed: {}",
                    operation_name,
                    attempt,
                    config.max_attempts,
                    e
                );
                last_error = Some(e);
            }
            Err(_elapsed) => {
                tracing::warn!(
                    "{}: attempt {}/{} timed out after {:?}",
                    operation_name,
                    attempt,
                    config.max_attempts,
                    config.timeout_per_attempt
                );
                if last_error.is_none() {
                    // Set a sentinel to indicate timeout was the last failure
                    return Err(CallWithRetryError::Timeout);
                }
            }
        }

        // Sleep before next attempt (unless this was the last attempt)
        if attempt < config.max_attempts {
            time::sleep(config.delay_between_attempts).await;
        }
    }

    // All attempts exhausted
    match last_error {
        Some(e) => Err(CallWithRetryError::OperationFailed(e)),
        None => Err(CallWithRetryError::Timeout),
    }
}

/// Errors that can occur during retry operations
#[derive(Debug)]
pub enum CallWithRetryError<E> {
    /// All attempts timed out
    Timeout,
    /// Operation failed with an error
    OperationFailed(E),
}

impl<E: std::fmt::Display> std::fmt::Display for CallWithRetryError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallWithRetryError::Timeout => write!(f, "all attempts timed out"),
            CallWithRetryError::OperationFailed(e) => write!(f, "operation failed: {}", e),
        }
    }
}

impl<E: std::fmt::Display + std::fmt::Debug> std::error::Error for CallWithRetryError<E> {}
