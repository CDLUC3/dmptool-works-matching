use std::collections::HashMap;

use polars::prelude::*;
use polars_core::series::Series;
use pyo3_polars::derive::polars_expr;
use serde_json;
use voca_rs::*;
use human_name::Name;
use log::{warn};

#[polars_expr(output_type=String)]
fn revert_inverted_index(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
    let out: StringChunked = ca.apply_into_string_amortized(|value: &str, output: &mut String| {
        // Parse JSON
        let data: HashMap<String, Vec<u32>> = match serde_json::from_str(value) {
            Ok(data) => data,
            Err(_) => return, // Skip invalid JSON rows
        };

        // Revert index
        let mut words: Vec<Option<String>> = Vec::new();
        for (word, positions) in &data {
            for pos in positions {
                let index = *pos as usize;
                if words.len() <= index {
                    words.resize(index + 1, None)
                }
                words[index] = Some(word.clone());
            }
        }

        // Write to string buffer
        let mut iter = words.into_iter().filter_map(|x| x);
        if let Some(first) = iter.next() {
            output.push_str(&first);
            for word in iter {
                output.push(' ');
                output.push_str(&word);
            }
        }
    });
    Ok(out.into_series())
}

fn parse_datacite_affiliations_output(input_fields: &[Field]) -> PolarsResult<Field> {
    let name = Field::new("name".into(), DataType::String);
    let affiliation_identifier = Field::new("affiliationIdentifier".into(), DataType::String);
    let affiliation_identifier_scheme = Field::new("affiliationIdentifierScheme".into(), DataType::String);
    let scheme_uri = Field::new("schemeUri".into(), DataType::String);

    let v: Vec<Field> = vec![
        name,
        affiliation_identifier,
        affiliation_identifier_scheme,
        scheme_uri,
    ];
    Ok(Field::new(
        input_fields[0].name.clone(),
        DataType::List(Box::new(DataType::Struct(v))),
    ))
}

#[polars_expr(output_type_func=parse_datacite_affiliations_output)]
fn parse_datacite_affiliations(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;

    fn empty_struct_series() -> Series {
        let name_series = Series::new("name".into(), vec![None::<String>; 0]);
        let affiliation_identifier_series = Series::new("affiliationIdentifier".into(), vec![None::<String>; 0]);
        let affiliation_identifier_scheme_series = Series::new("affiliationIdentifierScheme".into(), vec![None::<String>; 0]);
        let scheme_uri_series = Series::new("schemeUri".into(), vec![None::<String>; 0]);

        let fields = vec![
            name_series,
            affiliation_identifier_series,
            affiliation_identifier_scheme_series,
            scheme_uri_series,
        ];

        StructChunked::from_series("".into(), 0, fields.iter()).unwrap().into_series()
    }

    let list_ca: ListChunked = ca
        .into_iter()
        .map(|opt_str| {
            // Skip nulls
            let s: &str = match opt_str {
                Some(s) => s,
                None => {
                    return Some(empty_struct_series());
                },
            };

            // Parse JSON
            // Convert single JSON objects into a vector of objects
            // Keep JSON arrays as arrays
            // Skip other cases
            let json_array: Vec<serde_json::Value> = match serde_json::from_str(s) {
                Ok(serde_json::Value::Object(obj)) => vec![serde_json::Value::Object(obj)],
                Ok(serde_json::Value::Array(arr)) => arr,
                Ok(_) => return Some(empty_struct_series()), // Skip unexpected JSON types
                Err(_) => return Some(empty_struct_series()), // Skip invalid JSON
            };

            // Build vectors
            let mut name = Vec::with_capacity(json_array.len());
            let mut affiliation_identifier = Vec::with_capacity(json_array.len());
            let mut affiliation_identifier_scheme = Vec::with_capacity(json_array.len());
            let mut scheme_uri = Vec::with_capacity(json_array.len());
            for obj in &json_array {
                name.push(obj.get("name").and_then(|v| v.as_str()).map(str::to_owned));
                affiliation_identifier.push(
                    obj.get("affiliationIdentifier")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                );
                affiliation_identifier_scheme.push(
                    obj.get("affiliationIdentifierScheme")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                );
                scheme_uri.push(
                    obj.get("schemeUri")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                );
            }

            // Build each series
            let name_series = Series::new("name".into(), name);
            let affiliation_identifier_series = Series::new("affiliationIdentifier".into(), affiliation_identifier);
            let affiliation_identifier_scheme_series = Series::new("affiliationIdentifierScheme".into(), affiliation_identifier_scheme);
            let scheme_uri_series = Series::new("schemeUri".into(), scheme_uri);
            let fields = vec![
                name_series,
                affiliation_identifier_series,
                affiliation_identifier_scheme_series,
                scheme_uri_series,
            ];

            // Build list of structs
            StructChunked::from_series("".into(), json_array.len(), fields.iter()).ok().map(|s| s.into_series())
        })
        .collect::<ListChunked>();

    Ok(list_ca.into_series())
}

fn parse_datacite_name_identifiers_output(input_fields: &[Field]) -> PolarsResult<Field> {
    let name_identifier = Field::new("nameIdentifier".into(), DataType::String);
    let name_identifier_scheme = Field::new("nameIdentifierScheme".into(), DataType::String);
    let scheme_uri = Field::new("schemeUri".into(), DataType::String);

    let v: Vec<Field> = vec![name_identifier, name_identifier_scheme, scheme_uri];
    Ok(Field::new(
        input_fields[0].name.clone(),
        DataType::List(Box::new(DataType::Struct(v))),
    ))
}

#[polars_expr(output_type_func=parse_datacite_name_identifiers_output)]
fn parse_datacite_name_identifiers(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;

    fn empty_struct_series() -> Series {
        let name_identifier_series = Series::new("nameIdentifier".into(), vec![None::<String>; 0]);
        let name_identifier_scheme_series = Series::new("nameIdentifierScheme".into(), vec![None::<String>; 0]);
        let scheme_uri_series = Series::new("schemeUri".into(), vec![None::<String>; 0]);

        let fields = vec![
            name_identifier_series,
            name_identifier_scheme_series,
            scheme_uri_series,
        ];

        StructChunked::from_series("".into(), 0, fields.iter()).unwrap().into_series()
    }

    let list_ca: ListChunked = ca
        .into_iter()
        .map(|opt_str| {
            // Skip nulls
            let s: &str = match opt_str {
                Some(s) => s,
                None => {
                    return Some(empty_struct_series());
                },
            };

            // Parse JSON
            // Convert single JSON objects into a vector of objects
            // Keep JSON arrays as arrays
            // Skip other cases
            let json_array: Vec<serde_json::Value> = match serde_json::from_str(s) {
                Ok(serde_json::Value::Object(obj)) => vec![serde_json::Value::Object(obj)],
                Ok(serde_json::Value::Array(arr)) => arr,
                Ok(_) => return Some(empty_struct_series()), // Skip unexpected JSON types
                Err(_) => return Some(empty_struct_series()), // Skip invalid JSON
            };

            // Build vectors
            let mut name_identifier = Vec::with_capacity(json_array.len());
            let mut name_identifier_scheme = Vec::with_capacity(json_array.len());
            let mut scheme_uri = Vec::with_capacity(json_array.len());
            for obj in &json_array {
                name_identifier.push(
                    obj.get("nameIdentifier")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                );
                name_identifier_scheme.push(
                    obj.get("nameIdentifierScheme")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                );
                scheme_uri.push(
                    obj.get("schemeUri")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                );
            }

            // Build each series
            let name_identifier_series = Series::new("nameIdentifier".into(), name_identifier);
            let name_identifier_scheme_series = Series::new("nameIdentifierScheme".into(), name_identifier_scheme);
            let scheme_uri_series = Series::new("schemeUri".into(), scheme_uri);
            let fields = vec![
                name_identifier_series,
                name_identifier_scheme_series,
                scheme_uri_series,
            ];

            // Build list of structs
            StructChunked::from_series("".into(), json_array.len(), fields.iter()).ok().map(|s| s.into_series())
        })
        .collect::<ListChunked>();

    Ok(list_ca.into_series())
}

#[polars_expr(output_type=String)]
fn strip_markup(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
    let out: StringChunked = ca.apply_into_string_amortized(|value: &str, output: &mut String| {
        output.push_str(&strip::strip_tags(value));
    });
    Ok(out.into_series())
}

fn parse_name_output(input_fields: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(
        input_fields[0].name.clone(),
        DataType::Struct(vec![
            Field::new("first_initial".into(), DataType::String),
            Field::new("given_name".into(), DataType::String),
            Field::new("middle_initials".into(), DataType::String),
            Field::new("middle_names".into(), DataType::String),
            Field::new("surname".into(), DataType::String),
            Field::new("full".into(), DataType::String),
        ]),
    ))
}

fn fallback_parse_name(text: &str) -> (Option<String>, Option<String>, String) {
    let name_parts = if let Some((surname, given_name)) = text.split_once(",") {
        Some((given_name.trim(), surname.trim()))
    } else if let Some((given_name, surname)) = text.rsplit_once(" ") {
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

#[polars_expr(output_type_func=parse_name_output)]
fn parse_name(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
    let len = ca.len();

    let mut first_initials: Vec<Option<String>> = Vec::with_capacity(len);
    let mut given_names: Vec<Option<String>> = Vec::with_capacity(len);
    let mut middle_initials: Vec<Option<String>> = Vec::with_capacity(len);
    let mut middle_names: Vec<Option<String>> = Vec::with_capacity(len);
    let mut surnames: Vec<Option<String>> = Vec::with_capacity(len);
    let mut fulls: Vec<Option<String>> = Vec::with_capacity(len);

    for opt_s in ca {
        if let Some(s) = opt_s {
            if let Some(person) = Name::parse(s) {
                first_initials.push(Some(person.first_initial().to_string()));
                given_names.push(person.given_name().map(|s| s.to_string()));
                middle_initials.push(person.middle_initials().map(|s| s.to_string()));
                middle_names.push(person.middle_names().map(|v| v.join(" ")));
                surnames.push(Some(person.surname().to_string()));
                fulls.push(Some(person.display_full().into_owned()));
            } else {
                let (given_name, surname, full) = fallback_parse_name(s);
                warn!("fallback_parse_name: given_name='{:?}', surname='{:?}', full='{}'", given_name, surname, full);
                first_initials.push(None);
                given_names.push(given_name);
                middle_initials.push(None);
                middle_names.push(None);
                surnames.push(surname);
                fulls.push(Some(full));
            }
        } else {
            first_initials.push(None);
            given_names.push(None);
            middle_initials.push(None);
            middle_names.push(None);
            surnames.push(None);
            fulls.push(None);
        }
    }

    let first_initial_series = Series::new("first_initial".into(), first_initials);
    let given_name_series = Series::new("given_name".into(), given_names);
    let middle_initials_series = Series::new("middle_initials".into(), middle_initials);
    let middle_names_series = Series::new("middle_names".into(), middle_names);
    let surname_series = Series::new("surname".into(), surnames);
    let full_series = Series::new("full".into(), fulls);

    let fields: Vec<&Series> = vec![
        &first_initial_series,
        &given_name_series,
        &middle_initials_series,
        &middle_names_series,
        &surname_series,
        &full_series,
    ];
    let struct_chunked = StructChunked::from_series("".into(), len, fields.into_iter())?;
    Ok(struct_chunked.into_series())
}