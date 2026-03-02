use human_name::Name;
use std::collections::HashMap;
use log::warn;
use serde_json;
use strip_tags::strip_tags;
use unicode_segmentation::UnicodeSegmentation;

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

/// Checks if a string starts with a character that makes sense as an initial.
/// Returns true for alphabetic scripts (Latin, Cyrillic, Greek, Arabic, etc.)
/// and explicitly excludes CJK (Chinese, Japanese, Korean) ideographs/syllables.
pub fn has_meaningful_initials(text: &str) -> bool {
    let Some(c) = text.chars().next() else {
        return false;
    };

    // Must be a letter-like character
    if !c.is_alphabetic() {
        return false;
    }

    let u = c as u32;

    // Denylist for CJK Unicode Blocks
    let is_cjk = matches!(u,
        0x3000..=0x31FF | // Hiragana, Katakana, Bopomofo, Hangul Jamo
        0x3400..=0x4DBF | // CJK Unified Ideographs Extension A
        0x4E00..=0x9FFF | // CJK Unified Ideographs (Main Block)
        0xAC00..=0xD7AF | // Hangul Syllables
        0xF900..=0xFAFF | // CJK Compatibility Ideographs
        0x20000..=0x2A6DF // CJK Unified Ideographs Extension B, C, D, E, etc.
    );

    !is_cjk
}

/// Parses a raw name string into a structured `ParsedName` object, utilizing `human_name` with a fallback strategy.
pub fn parse_name(
  raw_given_name: Option<&str>,
  raw_surname: Option<&str>,
  raw_full: Option<&str>,
) -> ParsedName {
    let given = raw_given_name.map(str::trim).filter(|s| !s.is_empty());
    let surname = raw_surname.map(str::trim).filter(|s| !s.is_empty());
    let full = raw_full.map(str::trim).filter(|s| !s.is_empty());

    // If both given and surname are provided, build the final struct straight away
    if let (Some(g), Some(s)) = (given, surname) {
        let first_initial = if has_meaningful_initials(g) {
            g.graphemes(true).next().map(|grapheme| grapheme.to_uppercase())
        } else {
            None
        };

        // Use original full name if provided, otherwise build from given and surname
        // TODO: not ideal to combine first + last for many languages, but we will
        // just be using this for search so it is OK for now. The first initial, first name,
        // and surname are used for display so are more important to get right.
        let full_name = full
            .map(|f| f.to_string())
            .unwrap_or_else(|| format!("{} {}", g, s));

        return ParsedName {
            first_initial,
            given_name: Some(g.to_string()),
            middle_initials: None,
            middle_names: None,
            surname: Some(s.to_string()),
            full: Some(full_name),
        };
    }

    // If full, given and surname are all None then return None
    let Some(text_to_parse) = full.or(given).or(surname) else {
        return ParsedName {
            first_initial: None,
            given_name: None,
            middle_initials: None,
            middle_names: None,
            surname: None,
            full: None,
        };
    };

    if let Some(person) = Name::parse(text_to_parse) {
        return ParsedName {
            first_initial: Some(person.first_initial().to_string()),
            given_name: person.given_name().map(|v| v.to_string()),
            middle_initials: person.middle_initials().map(|v| v.to_string()),
            middle_names: person.middle_names().map(|v| v.join(" ")),
            surname: Some(person.surname().to_string()),
            full: Some(text_to_parse.to_string()),
        };
    }

    // Fallback if human_name fails
    let (parsed_given, parsed_surname, parsed_full) = fallback_parse_name(text_to_parse);
    warn!(
        "fallback_parse_name: given_name='{:?}', surname='{:?}', full='{}'",
        parsed_given, parsed_surname, parsed_full
    );

    ParsedName {
        first_initial: None,
        given_name: parsed_given,
        middle_initials: None,
        middle_names: None,
        surname: parsed_surname,
        full: Some(text_to_parse.to_string()),
    }
}

/// Reconstructs the original text from a JSON-serialized inverted index (mapping words to their positions).
pub fn revert_inverted_index(text: Option<&[u8]>, null_if_equals: Option<&[String]>) -> Option<String> {
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
        return None;
    }

    strip_markup(Some(trimmed), null_if_equals)
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