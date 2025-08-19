//! Panic handling utilities
//!
//! This module provides utilities for capturing and handling panic information
//! in the Numaflow SDK, including hooks and status building for gRPC responses.

use std::backtrace::Backtrace;
use std::panic;
use std::sync::Mutex;

use crate::shared::types::ENV_CONTAINER_TYPE;

/// Thread-safe storage for panic information
static PANIC_INFO: Mutex<Option<PanicInfo>> = Mutex::new(None);

/// Panic information captured by the panic hook
#[derive(Clone, Debug)]
pub struct PanicInfo {
    pub message: String,
    pub location: Option<String>,
    pub backtrace: String,
}

/// Initialize panic hook to capture detailed panic information
/// This should be called once when the server starts
pub fn init_panic_hook() {
    panic::set_hook(Box::new(|panic_info| {
        let message = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "Unknown panic".to_string()
        };

        let location = panic_info
            .location()
            .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()));

        // Capture backtrace immediately when panic occurs
        let backtrace = Backtrace::force_capture();

        let info = PanicInfo {
            message,
            location,
            backtrace: backtrace.to_string(),
        };

        // Store panic info for later retrieval (only if none exists yet)
        // We want to store the panic info only if it is the first panic
        if let Ok(mut panic_storage) = PANIC_INFO.lock() {
            if panic_storage.is_none() {
                *panic_storage = Some(info.clone());
            }
        }
    }));
}

/// Retrieve stored panic information without clearing it
/// Returns the first panic that occurred, if any
pub fn get_panic_info() -> Option<PanicInfo> {
    PANIC_INFO
        .lock()
        .ok()
        .and_then(|guard| guard.as_ref().cloned())
}

/// Create a formatted panic message including location information
fn format_panic_message(panic_info: &PanicInfo) -> String {
    match &panic_info.location {
        Some(location) => format!("{} at {}", panic_info.message, location),
        None => panic_info.message.clone(),
    }
}

/// This function creates a standardized tonic Status response when a UDF execution
/// encounters a panic, including detailed panic information and backtrace.
pub fn build_panic_status(panic_info: &PanicInfo) -> tonic::Status {
    use std::env;
    use tonic_types::{ErrorDetails, StatusExt};

    let panic_message = format_panic_message(panic_info);
    let status_msg = format!(
        "UDF_EXECUTION_ERROR({}): {}",
        env::var(ENV_CONTAINER_TYPE).unwrap_or_default(),
        panic_message
    );

    let details = ErrorDetails::with_debug_info(vec![], panic_info.backtrace.clone());
    tonic::Status::with_error_details(tonic::Code::Internal, status_msg, details)
}

#[cfg(all(test, feature = "test-panic"))]
mod tests {
    use super::*;
    use std::sync::Once;

    // Initialize panic hook only once for all tests
    static INIT_PANIC_HOOK: Once = Once::new();

    // Test helper to ensure panic hook is initialized once
    fn ensure_panic_hook_initialized() {
        INIT_PANIC_HOOK.call_once(|| {
            init_panic_hook();
        });
    }

    // Test helper to clear panic info between tests
    fn clear_panic_info_for_test() {
        if let Ok(mut guard) = PANIC_INFO.lock() {
            *guard = None;
        }
    }

    #[test]
    fn test_panic_hook_functionality() {
        // Ensure panic hook is initialized (only once across all tests)
        ensure_panic_hook_initialized();

        // Clear any existing panic info first
        clear_panic_info_for_test();

        // Verify no panic info exists initially
        assert!(
            get_panic_info().is_none(),
            "Panic info should be cleared initially"
        );

        // Test panic hook captures panic information
        let result = std::panic::catch_unwind(|| {
            panic!("Test panic message");
        });

        assert!(result.is_err(), "catch_unwind should capture the panic");

        // Give a small moment for the panic hook to execute
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Verify panic info was captured
        let panic_info = get_panic_info();
        assert!(
            panic_info.is_some(),
            "Panic info should be captured by the hook"
        );

        let info = panic_info.unwrap();
        assert_eq!(info.message, "Test panic message");
        assert!(info.location.is_some(), "Panic location should be captured");
        assert!(!info.backtrace.is_empty(), "Backtrace should not be empty");

        // Verify format_panic_message works correctly
        let formatted = format_panic_message(&info);
        assert!(formatted.contains("Test panic message"));
        assert!(formatted.contains("shared.rs"));

        // Verify panic info persists (second call returns same info - "first panic wins")
        let second_call = get_panic_info();
        assert!(second_call.is_some(), "Panic info should persist");
        assert_eq!(second_call.unwrap().message, "Test panic message");

        // Clean up for next test
        clear_panic_info_for_test();
    }
}
