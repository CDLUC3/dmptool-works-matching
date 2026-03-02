use human_name::Name;
use std::collections::HashMap;
use log::warn;
use serde_json;
use strip_tags::strip_tags;

#[derive(Debug, Clone)]
pub struct ParsedName {
    pub first_initial: Option<String>,
    pub given_name: Option<String>,
    pub middle_initials: Option<String>,
    pub middle_names: Option<String>,
    pub surname: Option<String>,
    pub full: Option<String>,
}

/// Attempts to parse a name string using simple splitting rules (comma or space) as a fallback mechanism.
fn fallback_parse_name(text: &str) -> (Option<String>, Option<String>, String) {
    let name_parts = if let Some((surname, given_name)) = text.split_once(',') {
        Some((given_name.trim(), surname.trim()))
    } else if let Some((given_name, surname)) = text.rsplit_once(' ') {
        Some((given_name.trim(), surname.trim()))
    } else {
        None
    };

    match name_parts {
        Some((given, surname)) => (
            Some(given.to_string()),
            Some(surname.to_string()),
            format!("{} {}", given, surname),
        ),
        None => (None, None, text.to_string()),
    }
}

/// Parses a raw name string into a structured `ParsedName` object, utilizing `human_name` with a fallback strategy.
pub fn parse_name(text: Option<&str>) -> ParsedName {
    let Some(s) = text.map(str::trim).filter(|s| !s.is_empty()) else {
        return ParsedName {
            first_initial: None,
            given_name: None,
            middle_initials: None,
            middle_names: None,
            surname: None,
            full: None,
        };
    };

    if let Some(person) = Name::parse(s) {
        return ParsedName {
            first_initial: Some(person.first_initial().to_string()),
            given_name: person.given_name().map(|v| v.to_string()),
            middle_initials: person.middle_initials().map(|v| v.to_string()),
            middle_names: person.middle_names().map(|v| v.join(" ")),
            surname: Some(person.surname().to_string()),
            full: Some(person.display_full().into_owned()),
        };
    }

    let (given_name, surname, full) = fallback_parse_name(s);
    warn!(
        "fallback_parse_name: given_name='{:?}', surname='{:?}', full='{}'",
        given_name, surname, full
    );

    ParsedName {
        first_initial: None,
        given_name,
        middle_initials: None,
        middle_names: None,
        surname,
        full: Some(full),
    }
}

/// Reconstructs the original text from a JSON-serialized inverted index (mapping words to their positions).
pub fn revert_inverted_index(text: Option<&[u8]>) -> Option<String> {
    let bytes = text?;
    if bytes.is_empty() {
        return None;
    }

    // Parse directly from bytes
    let data: HashMap<String, Vec<u32>> = match serde_json::from_slice(bytes) {
        Ok(v) => v,
        Err(e) => {
            warn!("revert_inverted_index: invalid json: {e}");
            return None;
        }
    };

    // Build words array by position
    let mut words: Vec<Option<String>> = Vec::new();
    for (word, positions) in data {
        for pos in positions {
            let idx = pos as usize;
            if words.len() <= idx {
                words.resize(idx + 1, None);
            }

            // To ensure determinism, when words share the same index, overwrite
            // if slot is not taken, or if it is taken, when the word is greater
            // alphabetically.
            let slot = &mut words[idx];
            if slot.is_none() || word > *slot.as_ref().unwrap() {
                *slot = Some(word.clone());
            }
        }
    }

    // Join in order (skip gaps)
    let mut iter = words.into_iter().flatten();
    let first = iter.next()?;
    let mut out = String::with_capacity(first.len() + 16);
    out.push_str(&first);
    for w in iter {
        out.push(' ');
        out.push_str(&w);
    }

    // Trim final result
    let trimmed = out.trim();

    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Removes HTML tags and surrounding whitespace from the input text, with an option to treat specific results as null.
pub fn strip_markup(text: Option<&str>, null_if_equals: Option<&[String]>) -> Option<String> {
    let s = text?;
    let stripped = strip_tags(s);
    let trimmed = stripped.trim();

    if trimmed.is_empty() {
        return None;
    }

    if let Some(values) = null_if_equals {
        if values.iter().any(|v| trimmed == v.as_str()) {
            return None;
        }
    }

    Some(trimmed.to_string())
}