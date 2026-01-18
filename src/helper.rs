use exn::{Result, ResultExt};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::ErrorStatus;

#[derive(Debug)]
pub struct JsonExtractError {
    pub message: String,
    pub status: ErrorStatus,
}

impl std::fmt::Display for JsonExtractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "json value extract error: {}", self.message)
    }
}

impl std::error::Error for JsonExtractError {}

/// Retrieves a value from a `serde_json::Value` by following a dot-separated path
/// and deserializes it into the requested type.
///
/// The path `xp` is split on `.` and applied step by step:
/// - When the current value is a JSON object, each path segment is treated as an object key.
/// - When the current value is a JSON array, the segment must be a valid `usize` index.
/// - Empty path segments are ignored.
///
/// # Errors
///
/// This function returns an error if:
/// 1. A path segment does not exist in a JSON object.
/// 2. A path segment is used as an array index but cannot be parsed as `usize`.
/// 3. An array index is out of bounds.
/// 4. A path segment attempts to descend into a non-container value.
/// 5. The final value cannot be deserialized into the requested type `T`.
///
/// # Examples
///
/// ```rust
/// use serde_json::json;
/// use datahugger::json_extract;
///
/// let value = json!({
///     "user": {
///         "id": 42,
///         "tags": ["admin", "active"]
///     }
/// });
///
/// let id: u64 = json_extract(&value, "user.id").expect("id is an u64");
/// let tag: String = json_extract(&value, "user.tags.0").expect("tag is a string");
/// ```
///
/// # Type Parameters
///
/// * `T` - The type to deserialize the final JSON value into.
pub fn json_extract<T>(value: &Value, path: &str) -> Result<T, JsonExtractError>
where
    T: DeserializeOwned,
{
    let mut current = value;

    for key in path.split('.').filter(|s| !s.is_empty()) {
        current = match current {
            Value::Object(map) => map.get(key).ok_or(JsonExtractError {
                message: format!("'{key}' not found in object at path '{path}'"),
                status: ErrorStatus::Permanent,
            })?,
            Value::Array(arr) => {
                let idx = key.parse::<usize>().or_raise(|| JsonExtractError {
                    message: format!("key '{key}' cannot parse to an index at path '{path}'"),
                    status: ErrorStatus::Permanent,
                })?;
                arr.get(idx).ok_or(JsonExtractError {
                    message: format!("array index {idx} out of bounds"),
                    status: ErrorStatus::Permanent,
                })?
            }
            _ => Err(JsonExtractError {
                message: format!(
                    "key '{key}' cannot descend into non-container value at path '{path}'"
                ),
                status: ErrorStatus::Permanent,
            })?,
        };
    }
    let value: T = serde_json::from_value::<T>(current.clone()).or_raise(|| JsonExtractError {
        message: format!("failed to deserialize value at path '{path}'"),
        status: ErrorStatus::Permanent,
    })?;

    Ok(value)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_json_extract_default() {
        let value = json!({
            "data": [
                { "name": "bob", "num": 5 }
            ]
        });
        let xp = "data.0.name";
        let v: String = json_extract(&value, xp).unwrap();
        assert_eq!(v, "bob");

        let xp = "data.0.num";
        let v: u64 = json_extract(&value, xp).unwrap();
        assert_eq!(v, 5);
    }

    #[test]
    fn test_json_extract_missing_path() {
        let value = serde_json::json!({
            "data": []
        });

        let xp = "data.0.name";
        let err = json_extract::<String>(&value, xp).unwrap_err();
        assert!(err.to_string().contains("out of bounds"));
    }

    #[test]
    fn test_json_extract_wrong_container() {
        let value = serde_json::json!({
            "data": "not an array"
        });

        let xp = "data.0";
        let err = json_extract::<String>(&value, xp).unwrap_err();
        assert!(err.to_string().contains("cannot descend"));
    }

    #[test]
    fn test_json_extract_deserialize_error() {
        let value = serde_json::json!({
            "data": { "id": "not a number" }
        });

        let xp = "data.id";
        let err = json_extract::<i64>(&value, xp).unwrap_err();
        assert!(err.to_string().contains("deserialize"));
    }
}
