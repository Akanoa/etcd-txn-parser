//! A compare operation.
//!
//! See the [Compare API](https://github.com/etcd-io/etcd/blob/main/etcdctl/README.md#txn-options) for
//! more information.

use crate::operation::Data;
use elyze::acceptor::Acceptor;
use elyze::bytes::components::groups::GroupKind;
use elyze::bytes::primitives::number::Number;
use elyze::bytes::primitives::whitespace::OptionalWhitespaces;
use elyze::bytes::token::Token;
use elyze::errors::{ParseError, ParseResult};
use elyze::peek::{peek, Until, UntilEnd};
use elyze::recognizer::Recognizer;
use elyze::scanner::Scanner;
use elyze::visitor::Visitor;

//----------------------------------------------------------------------------
// Key
//----------------------------------------------------------------------------

struct Key<'a>(&'a [u8]);

impl<'a> Visitor<'a, u8> for Key<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        let key_slice =
            peek(GroupKind::Parenthesis, scanner)?.ok_or(ParseError::UnexpectedToken)?;
        let mut inner_scanner = Scanner::new(key_slice.peeked_slice());
        let key = Data::accept(&mut inner_scanner)?.data;
        scanner.bump_by(key_slice.end_slice);

        Ok(Key(key))
    }
}

// ----------------------------------------------------------------------------
// OpType
// ----------------------------------------------------------------------------

/// A comparison operator.
#[derive(Debug, PartialEq)]
pub enum OpType {
    /// Equal
    Equal,
    /// Greater than
    GreaterThan,
    /// Less than
    LessThan,
}

impl<'a> Visitor<'a, u8> for OpType {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        let operator = Recognizer::new(scanner)
            .try_or(Token::Equal)?
            .try_or(Token::GreaterThan)?
            .try_or(Token::LessThan)?
            .finish()
            .ok_or(ParseError::UnexpectedToken)?;
        match operator {
            Token::Equal => Ok(OpType::Equal),
            Token::GreaterThan => Ok(OpType::GreaterThan),
            Token::LessThan => Ok(OpType::LessThan),
            _ => unreachable!("Recognizer should have caught this"),
        }
    }
}

// ----------------------------------------------------------------------------
// Compare create revision
// ----------------------------------------------------------------------------

/// A create revision compare operation.
#[derive(Debug, PartialEq)]
pub struct CreateRevision<'a> {
    /// The key to compare.
    key: &'a [u8],
    /// The value to compare with.
    value: u64,
    /// The comparison operator.
    op: OpType,
}

impl<'a> Visitor<'a, u8> for CreateRevision<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        OptionalWhitespaces::accept(scanner)?;
        let prefix = peek(Until::new(Token::OpenParen), scanner)?
            .ok_or(ParseError::UnexpectedToken)?
            .data();
        if prefix.trim_ascii_end() != b"c" && prefix != b"create".trim_ascii_end() {
            return Err(ParseError::UnexpectedToken);
        }

        // Advance the scanner by the size of the prefix
        scanner.bump_by(prefix.len());

        let key = Key::accept(scanner)?.0;

        OptionalWhitespaces::accept(scanner)?;
        let op = OpType::accept(scanner)?;
        OptionalWhitespaces::accept(scanner)?;
        let value = Number::accept(scanner)?.0;

        Ok(CreateRevision { key, value, op })
    }
}

// ----------------------------------------------------------------------------
// Compare mod revision
// ----------------------------------------------------------------------------

/// A modify revision compare operation.
#[derive(Debug, PartialEq)]
pub struct ModRevision<'a> {
    /// The key to compare.
    pub key: &'a [u8],
    /// The value to compare with.
    pub value: u64,
    /// The comparison operator.
    pub op: OpType,
}

impl<'a> Visitor<'a, u8> for ModRevision<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        OptionalWhitespaces::accept(scanner)?;
        let prefix = peek(Until::new(Token::OpenParen), scanner)?
            .ok_or(ParseError::UnexpectedToken)?
            .data();
        if prefix.trim_ascii_end() != b"m" && prefix != b"mod".trim_ascii_end() {
            return Err(ParseError::UnexpectedToken);
        }

        // Advance the scanner by the size of the prefix
        scanner.bump_by(prefix.len());

        let key = Key::accept(scanner)?.0;

        OptionalWhitespaces::accept(scanner)?;
        let op = OpType::accept(scanner)?;
        OptionalWhitespaces::accept(scanner)?;
        let value = Number::accept(scanner)?.0;

        Ok(ModRevision { key, value, op })
    }
}

// ----------------------------------------------------------------------------
// Compare value
// ----------------------------------------------------------------------------

/// A value compare operation.
#[derive(Debug, PartialEq)]
pub struct Value<'a> {
    /// The key to compare.
    key: &'a [u8],
    /// The value to compare with.
    value: &'a [u8],
    /// The comparison operator.
    op: OpType,
}

impl<'a> Visitor<'a, u8> for Value<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        OptionalWhitespaces::accept(scanner)?;
        let prefix = peek(Until::new(Token::OpenParen), scanner)?
            .ok_or(ParseError::UnexpectedToken)?
            .data();
        if prefix.trim_ascii_end() != b"val" && prefix != b"value".trim_ascii_end() {
            return Err(ParseError::UnexpectedToken);
        }

        // Advance the scanner by the size of the prefix
        scanner.bump_by(prefix.len());

        let key = Key::accept(scanner)?.0;

        OptionalWhitespaces::accept(scanner)?;
        let op = OpType::accept(scanner)?;
        OptionalWhitespaces::accept(scanner)?;
        let value = peek(UntilEnd::default(), scanner)?
            .ok_or(ParseError::UnexpectedToken)?
            .data;

        Ok(Value { key, value, op })
    }
}

// ----------------------------------------------------------------------------
// Compare version
// ----------------------------------------------------------------------------

/// A version compare operation.
#[derive(Debug, PartialEq)]
pub struct Version<'a> {
    /// The key to compare.
    key: &'a [u8],
    /// The value to compare with.
    value: u64,
    /// The comparison operator.
    op: OpType,
}

impl<'a> Visitor<'a, u8> for Version<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        OptionalWhitespaces::accept(scanner)?;
        let prefix = peek(Until::new(Token::OpenParen), scanner)?
            .ok_or(ParseError::UnexpectedToken)?
            .data();
        if prefix.trim_ascii_end() != b"ver" && prefix != b"version".trim_ascii_end() {
            return Err(ParseError::UnexpectedToken);
        }

        // Advance the scanner by the size of the prefix
        scanner.bump_by(prefix.len());

        let key = Key::accept(scanner)?.0;

        OptionalWhitespaces::accept(scanner)?;
        let op = OpType::accept(scanner)?;
        OptionalWhitespaces::accept(scanner)?;
        let value = Number::accept(scanner)?.0;

        Ok(Version { key, value, op })
    }
}

// ----------------------------------------------------------------------------
// Compare lease
// ----------------------------------------------------------------------------

/// A lease compare operation.
#[derive(Debug, PartialEq)]
pub struct Lease<'a> {
    /// The key to compare.
    key: &'a [u8],
    /// The value to compare with.
    value: u64,
    /// The comparison operator.
    op: OpType,
}

impl<'a> Visitor<'a, u8> for Lease<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        OptionalWhitespaces::accept(scanner)?;
        let prefix = peek(Until::new(Token::OpenParen), scanner)?
            .ok_or(ParseError::UnexpectedToken)?
            .data();
        if prefix.trim_ascii_end() != b"lease" {
            return Err(ParseError::UnexpectedToken);
        }

        // Advance the scanner by the size of the prefix
        scanner.bump_by(prefix.len());

        let key = Key::accept(scanner)?.0;

        OptionalWhitespaces::accept(scanner)?;
        let op = OpType::accept(scanner)?;
        OptionalWhitespaces::accept(scanner)?;
        let value = Number::accept(scanner)?.0;

        Ok(Lease { key, value, op })
    }
}

//----------------------------------------------------------------------------
// Compare
//----------------------------------------------------------------------------

/// A compare operation.
#[derive(Debug, PartialEq)]
pub enum Compare<'a> {
    /// A create revision compare operation.
    CreateRevision(CreateRevision<'a>),
    /// A modify revision compare operation.
    ModRevision(ModRevision<'a>),
    /// A value compare operation.
    Value(Value<'a>),
    /// A version compare operation.
    Version(Version<'a>),
    /// A lease compare operation.
    Lease(Lease<'a>),
}

impl<'a> Visitor<'a, u8> for Compare<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        let compare = Acceptor::new(scanner)
            .try_or(Compare::ModRevision)?
            .try_or(Compare::CreateRevision)?
            .try_or(Compare::Value)?
            .try_or(Compare::Version)?
            .try_or(Compare::Lease)?
            .finish()
            .ok_or(ParseError::UnexpectedToken)?;

        Ok(compare)
    }
}

#[cfg(test)]
mod tests {
    use crate::compare::{Compare, CreateRevision, Lease, ModRevision, OpType, Value, Version};
    use elyze::scanner::Scanner;
    use elyze::visitor::Visitor;

    #[test]
    fn test_create_revision() {
        let data = b"create(key) = 1";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::CreateRevision(CreateRevision {
                key: b"key",
                value: 1,
                op: OpType::Equal
            }))
        ));

        let data = b"create(\"key with spaces\") = 51515221";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::CreateRevision(CreateRevision {
                key: b"key with spaces",
                value: 51515221,
                op: OpType::Equal
            }))
        ));

        let data = b"c(key) = 1";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::CreateRevision(CreateRevision {
                key: b"key",
                value: 1,
                op: OpType::Equal
            }))
        ));

        let data = b"c(key) > 1";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::CreateRevision(CreateRevision {
                key: b"key",
                value: 1,
                op: OpType::GreaterThan
            }))
        ));

        let data = b"c(key) < 1";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::CreateRevision(CreateRevision {
                key: b"key",
                value: 1,
                op: OpType::LessThan
            }))
        ));
    }

    #[test]
    fn test_mod_revision() {
        let data = b"mod(key) = 1";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::ModRevision(ModRevision {
                key: b"key",
                value: 1,
                op: OpType::Equal
            }))
        ));

        let data = b"mod(\"key with spaces\") = 51515221";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::ModRevision(ModRevision {
                key: b"key with spaces",
                value: 51515221,
                op: OpType::Equal
            }))
        ));

        let data = b"m(key) = 1";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::ModRevision(ModRevision {
                key: b"key",
                value: 1,
                op: OpType::Equal
            }))
        ));

        let data = b"m(key) > 1";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::ModRevision(ModRevision {
                key: b"key",
                value: 1,
                op: OpType::GreaterThan
            }))
        ));

        let data = b"m(key) < 1";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::ModRevision(ModRevision {
                key: b"key",
                value: 1,
                op: OpType::LessThan
            }))
        ));
    }

    #[test]
    fn test_value() {
        let data = b"value(key) = data";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::Value(Value {
                key: b"key",
                value: b"data",
                op: OpType::Equal
            }))
        ));

        let data = b"value(\"key with spaces\") = data";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::Value(Value {
                key: b"key with spaces",
                value: b"data",
                op: OpType::Equal
            }))
        ));

        let data = b"val(key) = data";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::Value(Value {
                key: b"key",
                value: b"data",
                op: OpType::Equal
            }))
        ));

        let data = b"val(key) > data";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::Value(Value {
                key: b"key",
                value: b"data",
                op: OpType::GreaterThan
            }))
        ));

        let data = b"val(key) < data";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::Value(Value {
                key: b"key",
                value: b"data",
                op: OpType::LessThan
            }))
        ));
    }

    #[test]
    fn test_version() {
        let data = b"version(key) = 1";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::Version(Version {
                key: b"key",
                value: 1,
                op: OpType::Equal
            }))
        ));

        let data = b"version(\"key with spaces\") = 51515221";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::Version(Version {
                key: b"key with spaces",
                value: 51515221,
                op: OpType::Equal
            }))
        ));

        let data = b"ver(key) = 1";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::Version(Version {
                key: b"key",
                value: 1,
                op: OpType::Equal
            }))
        ));

        let data = b"ver(key) > 1";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::Version(Version {
                key: b"key",
                value: 1,
                op: OpType::GreaterThan
            }))
        ));

        let data = b"ver(key) < 1";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::Version(Version {
                key: b"key",
                value: 1,
                op: OpType::LessThan
            }))
        ));
    }

    #[test]
    fn test_lease() {
        let data = b"lease(key) = 1";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::Lease(Lease {
                key: b"key",
                value: 1,
                op: OpType::Equal
            }))
        ));

        let data = b"lease(\"key with spaces\") = 51515221";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::Lease(Lease {
                key: b"key with spaces",
                value: 51515221,
                op: OpType::Equal
            }))
        ));

        let data = b"lease(key) > 1";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::Lease(Lease {
                key: b"key",
                value: 1,
                op: OpType::GreaterThan
            }))
        ));

        let data = b"lease(key) < 1";
        let mut scanner = Scanner::new(data);
        let result = Compare::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(Compare::Lease(Lease {
                key: b"key",
                value: 1,
                op: OpType::LessThan
            }))
        ));
    }
}
