//! Transactional operations

use elyze::acceptor::Acceptor;
use elyze::bytes::components::groups::GroupKind;
use elyze::bytes::primitives::string::DataString;
use elyze::bytes::primitives::whitespace::OptionalWhitespaces;
use elyze::bytes::token::Token;
use elyze::errors::{ParseError, ParseResult};
use elyze::peek::{peek, UntilEnd};
use elyze::peeker::Peeker;
use elyze::scanner::Scanner;
use elyze::visitor::Visitor;

// ----------------------------------------------------------------------------
// QuotedString
// ----------------------------------------------------------------------------

struct QuotedString<'a>(&'a [u8]);

impl<'a> Visitor<'a, u8> for QuotedString<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        let peeked = peek(GroupKind::DoubleQuotes, scanner)?.ok_or(ParseError::UnexpectedToken)?;
        scanner.bump_by(peeked.end_slice);
        Ok(QuotedString(peeked.peeked_slice()))
    }
}

//----------------------------------------------------------------------------
// UnquotedString
//----------------------------------------------------------------------------

struct UnquotedString<'a>(&'a [u8]);

impl<'a> Visitor<'a, u8> for UnquotedString<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        let peeked = {
            let peeked = Peeker::new(scanner)
                .add_peekable(Token::Whitespace)
                .add_peekable(UntilEnd::default())
                .peek()?
                .ok_or(ParseError::UnexpectedToken)?;
            peeked
        };

        scanner.bump_by(peeked.end_slice);
        Ok(UnquotedString(peeked.peeked_slice()))
    }
}

//----------------------------------------------------------------------------
// Data
//----------------------------------------------------------------------------

pub struct Data<'a> {
    pub(crate) data: &'a [u8],
}

impl<'a> Visitor<'a, u8> for Data<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        let accepted = Acceptor::new(scanner)
            .try_or(|x: QuotedString| x.0)?
            .try_or(|x: UnquotedString| x.0)?
            .finish()
            .ok_or(ParseError::UnexpectedToken)?;
        Ok(Data { data: accepted })
    }
}

// ----------------------------------------------------------------------------
// Put Operation
// ----------------------------------------------------------------------------

/// A put operation.
#[derive(Debug, PartialEq)]
pub struct PutData<'a> {
    /// The key to put.
    pub key: &'a [u8],
    /// The value to put.
    pub value: &'a [u8],
}

impl<'a> Visitor<'a, u8> for PutData<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        OptionalWhitespaces::accept(scanner)?;
        let command = DataString::<&str>::accept(scanner)?.0;
        if command != "put" {
            return Err(ParseError::UnexpectedToken);
        }
        OptionalWhitespaces::accept(scanner)?;
        let key = Data::accept(scanner)?.data;
        OptionalWhitespaces::accept(scanner)?;
        let value = Data::accept(scanner)?.data;
        OptionalWhitespaces::accept(scanner)?;
        Ok(PutData { key, value })
    }
}

// ----------------------------------------------------------------------------
// Delete Operation
// ----------------------------------------------------------------------------

/// A delete operation.
#[derive(Debug, PartialEq)]
pub struct DeleteData<'a> {
    /// The key to delete.
    pub key: &'a [u8],
}

impl<'a> Visitor<'a, u8> for DeleteData<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        OptionalWhitespaces::accept(scanner)?;
        let command = DataString::<&str>::accept(scanner)?.0;
        if command != "del" {
            return Err(ParseError::UnexpectedToken);
        }
        OptionalWhitespaces::accept(scanner)?;
        let until_ln = Peeker::new(scanner)
            .add_peekable(Token::Ln)
            .add_peekable(UntilEnd::default())
            .peek()?
            .ok_or(ParseError::UnexpectedToken)?;
        let mut scanner_until_ln = Scanner::new(until_ln.peeked_slice());

        let key = Data::accept(&mut scanner_until_ln)?.data;
        scanner.bump_by(scanner_until_ln.current_position());
        OptionalWhitespaces::accept(scanner)?;

        Ok(DeleteData { key })
    }
}

// ----------------------------------------------------------------------------
// Get Operation
// ----------------------------------------------------------------------------

/// A get operation.
#[derive(Debug, PartialEq)]
pub struct GetData<'a> {
    /// The key to get.
    pub key: &'a [u8],
}

impl<'a> Visitor<'a, u8> for GetData<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        OptionalWhitespaces::accept(scanner)?;
        let command = DataString::<&str>::accept(scanner)?.0;
        if command != "get" {
            return Err(ParseError::UnexpectedToken);
        }

        OptionalWhitespaces::accept(scanner)?;

        let until_ln = Peeker::new(scanner)
            .add_peekable(Token::Ln)
            .add_peekable(UntilEnd::default())
            .peek()?
            .ok_or(ParseError::UnexpectedToken)?;
        let mut scanner_until_ln = Scanner::new(until_ln.peeked_slice());

        let key = Data::accept(&mut scanner_until_ln)?.data;
        scanner.bump_by(scanner_until_ln.current_position());
        OptionalWhitespaces::accept(scanner)?;

        Ok(GetData { key })
    }
}

// ----------------------------------------------------------------------------
// Operation
// ----------------------------------------------------------------------------

/// A transactional operation.
#[derive(Debug, PartialEq)]
pub enum Operation<'a> {
    /// A put operation.
    Put(PutData<'a>),
    /// A delete operation.
    Delete(DeleteData<'a>),
    /// A get operation.
    Get(GetData<'a>),
}

impl<'a> Visitor<'a, u8> for Operation<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        let operation = Acceptor::new(scanner)
            .try_or(Operation::Put)?
            .try_or(Operation::Delete)?
            .try_or(Operation::Get)?
            .finish()
            .ok_or(ParseError::UnexpectedToken)?;
        Ok(operation)
    }
}

#[cfg(test)]
mod tests {
    use crate::operation::GetData;
    use elyze::visitor::Visitor;

    #[test]
    fn test_get_data() {
        let data = b"get \"key\"";
        let mut scanner = elyze::scanner::Scanner::new(data);
        let result = super::GetData::accept(&mut scanner);
        assert!(matches!(result, Ok(GetData { key: b"key" })));

        let data = b"get key";
        let mut scanner = elyze::scanner::Scanner::new(data);
        let result = super::GetData::accept(&mut scanner);
        assert!(matches!(result, Ok(GetData { key: b"key" })));
    }

    #[test]
    fn test_delete_data() {
        let data = b"del \"key\"";
        let mut scanner = elyze::scanner::Scanner::new(data);
        let result = super::DeleteData::accept(&mut scanner);
        assert!(matches!(result, Ok(super::DeleteData { key: b"key" })));

        let data = b"del key";
        let mut scanner = elyze::scanner::Scanner::new(data);
        let result = super::DeleteData::accept(&mut scanner);
        assert!(matches!(result, Ok(super::DeleteData { key: b"key" })));
    }

    #[test]
    fn test_put_data() {
        let data = b"put \"key\" \"value\"";
        let mut scanner = elyze::scanner::Scanner::new(data);
        let result = super::PutData::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(super::PutData {
                key: b"key",
                value: b"value"
            })
        ));

        let data = b"put key value";
        let mut scanner = elyze::scanner::Scanner::new(data);
        let result = super::PutData::accept(&mut scanner);

        if let Ok(result) = &result {
            println!("{:?}", String::from_utf8_lossy(result.key));
            println!("{:?}", String::from_utf8_lossy(result.value));
        }

        assert!(matches!(
            result,
            Ok(super::PutData {
                key: b"key",
                value: b"value"
            })
        ));
    }
}
