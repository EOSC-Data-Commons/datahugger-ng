use exn::{Exn, Result, ResultExt};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::ErrorStatus;

#[derive(Debug)]
#[allow(dead_code)]
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
/// ```ignore
/// ```rust
/// use serde_json::json;
///
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
pub(crate) fn json_extract<T>(value: &Value, path: &str) -> Result<T, JsonExtractError>
where
    T: DeserializeOwned,
{
    json_extract_opt(value, path)?.ok_or_else(|| {
        Exn::new(JsonExtractError {
            message: format!("path '{path}' not found"),
            status: ErrorStatus::Permanent,
        })
    })
}

pub(crate) fn json_extract_opt<T>(value: &Value, path: &str) -> Result<Option<T>, JsonExtractError>
where
    T: DeserializeOwned,
{
    let mut current = value;

    // Navigate the path - return None if path doesn't exist
    for key in path.split('.').filter(|s| !s.is_empty()) {
        current = match current {
            Value::Object(map) => match map.get(key) {
                Some(v) => v,
                None => return Ok(None), // Path doesn't exist
            },
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
            _ => return Ok(None), // Can't descend into non-container
        };
    }

    // Path exists, try to deserialize - error if wrong type
    let value: T = serde_json::from_value::<T>(current.clone()).or_raise(|| JsonExtractError {
        message: format!("failed to deserialize value at path '{path}'"),
        status: ErrorStatus::Permanent,
    })?;

    Ok(Some(value))
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
        assert!(
            err.message
                .to_string()
                .contains("array index 0 out of bounds"),
            "{}",
            err.message
        );
    }

    #[test]
    fn test_json_extract_missing_path_optional() {
        let value = serde_json::json!({
            "data": []
        });

        let xp = "data.0.name";
        let err = json_extract_opt::<String>(&value, xp).unwrap_err();
        assert!(
            err.message
                .to_string()
                .contains("array index 0 out of bounds"),
            "{}",
            err.message
        );
    }

    #[test]
    fn test_json_extract_wrong_container() {
        let value = serde_json::json!({
            "data": "not an array"
        });

        let xp = "data.0";

        let res = json_extract::<String>(&value, xp).unwrap_err();
        assert!(res.message.to_string().contains("path 'data.0' not found"));
    }

    #[test]
    fn test_json_extract_array_with_non_numeric_id() {
        let value = json!({
            "data": [
                1
            ]
        });

        let err = json_extract::<i64>(&value, "data.a").unwrap_err();
        assert!(
            err.message
                .to_string()
                .contains("key 'a' cannot parse to an index at path 'data.a'"),
            "{}",
            err.message
        );
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

    #[test]
    fn test_json_extract_optional_value() {
        let value = json!({
            "persons": [
                {
                    "name": "John Doe",
                    "age": 43,
                },
                {
                    "name": "Jane Doe"
                }
            ]
        });

        let xp1 = "persons.0.age";
        let xp2 = "persons.1.age";

        let age1: Option<i64> = json_extract_opt(&value, xp1).unwrap();
        let age2: Option<i64> = json_extract_opt(&value, xp2).unwrap();

        assert_eq!(age1, Some(43));
        assert_eq!(age2, None);
    }
}
