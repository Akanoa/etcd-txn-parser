//! Transactional operations

use noa_parser::acceptor::Acceptor;
use noa_parser::bytes::components::groups::GroupKind;
use noa_parser::bytes::primitives::string::DataString;
use noa_parser::bytes::primitives::whitespace::OptionalWhitespaces;
use noa_parser::bytes::token::Token;
use noa_parser::errors::{ParseError, ParseResult};
use noa_parser::peek::{peek, Until, UntilEnd};
use noa_parser::peeker::Peeker;
use noa_parser::scanner::Scanner;
use noa_parser::visitor::Visitor;

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
        let mut inner_scanner = Scanner::new(&scanner.data()[scanner.current_position()..]);

        let peeked = Peeker::new(&mut inner_scanner)
            .add_peekable(Until::new(Token::Whitespace))
            .add_peekable(UntilEnd::default())
            .peek()?
            .ok_or(ParseError::UnexpectedToken)?;

        // L'emprunt immutable de Peeker est terminé ici
        let current_position = scanner.current_position();
        let data =
            UnquotedString(&scanner.data()[current_position..current_position + peeked.end_slice]);

        scanner.bump_by(peeked.end_slice);
        Ok(data)
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
    key: &'a [u8],
}

impl<'a> Visitor<'a, u8> for DeleteData<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        OptionalWhitespaces::accept(scanner)?;
        let command = DataString::<&str>::accept(scanner)?.0;
        if command != "del" {
            return Err(ParseError::UnexpectedToken);
        }
        OptionalWhitespaces::accept(scanner)?;
        let key = Data::accept(scanner)?.data;
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
    key: &'a [u8],
}

impl<'a> Visitor<'a, u8> for GetData<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        OptionalWhitespaces::accept(scanner)?;
        let command = DataString::<&str>::accept(scanner)?.0;
        if command != "get" {
            return Err(ParseError::UnexpectedToken);
        }
        OptionalWhitespaces::accept(scanner)?;
        let key = Data::accept(scanner)?.data;
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
    use noa_parser::visitor::Visitor;

    #[test]
    fn test_get_data() {
        let data = b"get \"key\"";
        let mut scanner = noa_parser::scanner::Scanner::new(data);
        let result = super::GetData::accept(&mut scanner);
        assert!(matches!(result, Ok(GetData { key: b"key" })));

        let data = b"get key";
        let mut scanner = noa_parser::scanner::Scanner::new(data);
        let result = super::GetData::accept(&mut scanner);
        assert!(matches!(result, Ok(GetData { key: b"key" })));
    }

    #[test]
    fn test_delete_data() {
        let data = b"del \"key\"";
        let mut scanner = noa_parser::scanner::Scanner::new(data);
        let result = super::DeleteData::accept(&mut scanner);
        assert!(matches!(result, Ok(super::DeleteData { key: b"key" })));

        let data = b"del key";
        let mut scanner = noa_parser::scanner::Scanner::new(data);
        let result = super::DeleteData::accept(&mut scanner);
        assert!(matches!(result, Ok(super::DeleteData { key: b"key" })));
    }

    #[test]
    fn test_put_data() {
        let data = b"put \"key\" \"value\"";
        let mut scanner = noa_parser::scanner::Scanner::new(data);
        let result = super::PutData::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(super::PutData {
                key: b"key",
                value: b"value"
            })
        ));

        let data = b"put key value";
        let mut scanner = noa_parser::scanner::Scanner::new(data);
        let result = super::PutData::accept(&mut scanner);
        assert!(matches!(
            result,
            Ok(super::PutData {
                key: b"key",
                value: b"value"
            })
        ));
    }
}
