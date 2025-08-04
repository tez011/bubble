use crate::Datum;
use crate::io::InputPort;

#[derive(Debug, Clone)]
pub struct SyntaxObject {
    e: Datum,
    children: Vec<SyntaxObject>,
    pub source_location: (usize, usize),
}
impl SyntaxObject {
    fn simple(e: Datum, source_location: (usize, usize)) -> Self {
        Self::compound(e, Vec::new(), source_location)
    }
    fn compound(e: Datum, children: Vec<SyntaxObject>, source_location: (usize, usize)) -> Self {
        SyntaxObject {
            e,
            children,
            source_location
        }
    }
}
impl From<SyntaxObject> for Datum {
    fn from(s: SyntaxObject) -> Datum {
        match s.e {
            Datum::List(_, b) => Datum::List(s.children.into_iter().map(|stx| Datum::from(stx)).collect(), b),
            Datum::Vector(_) => Datum::Vector(s.children.into_iter().map(|stx| Datum::from(stx)).collect()),
            d => d,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Identifier(String),
    Boolean(bool),
    Number(String, TokenTag),
    Character(String),
    String(String),
    Open,
    Close,
    Vector,
    Bytes,
    Quote,
    Backquote,
    Unquote,
    UnquoteSplicing,
    Dot,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenTag {
    Integer,
    Rational,
    Decimal,
    Infinity,
    NaN,
}

#[derive(Debug)]
pub enum ParseError {
    NoToken,
    UnexpectedToken,
    IoError(std::io::Error),
    InvalidNumber(&'static str),
    InvalidCharacter,
}
impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::NoToken => write!(f, "No token found"),
            ParseError::UnexpectedToken => write!(f, "Unexpected token"),
            ParseError::IoError(e) => write!(f, "I/O error: {}", e),
            ParseError::InvalidNumber(msg) => write!(f, "Invalid number: {}", msg),
            ParseError::InvalidCharacter => write!(f, "Invalid character"),
        }
    }
}

/******** parser ********/

pub fn next(port: &mut InputPort) -> Result<SyntaxObject, ParseError> {
    next_datum(port, None)
}

fn next_datum(port: &mut InputPort, token: Option<Token>) -> Result<SyntaxObject, ParseError> {
    let source_location = port.location();
    let token = match token {
        Some(t) => Some(t),
        None => next_token(port).map_err(ParseError::IoError)?
    };
    match token {
        Some(Token::Boolean(b)) => Ok(SyntaxObject::simple(Datum::Boolean(b), source_location)),
        Some(Token::Number(s, st)) => Ok(SyntaxObject::simple(parse_number(s, st)?, source_location)),
        Some(Token::Character(s)) => {
            let c = parse_character(s).map_err(|_| ParseError::InvalidCharacter)?;
            Ok(SyntaxObject::simple(Datum::Character(c), source_location))
        },
        Some(Token::String(s)) => {
            let s = parse_string(s).map_err(|_| ParseError::InvalidCharacter)?;
            Ok(SyntaxObject::simple(Datum::String(s), source_location))
        },
        Some(Token::Identifier(s)) => {
            let s = parse_identifier(s).map_err(|_| ParseError::InvalidCharacter)?;
            Ok(SyntaxObject::simple(Datum::Symbol(s), source_location))
        },
        Some(Token::Open) => {
            let mut children = Vec::new();
            loop {
                match next_token(port).map_err(ParseError::IoError)? {
                    Some(Token::Close) => return Ok(SyntaxObject::compound(Datum::List(Vec::new(), false), children, source_location)),
                    Some(Token::Dot) => {
                        children.push(next_datum(port, None)?);
                        match next_token(port).map_err(ParseError::IoError)? {
                            Some(Token::Close) => return Ok(SyntaxObject::compound(Datum::List(Vec::new(), true), children, source_location)),
                            _ => return Err(ParseError::UnexpectedToken),
                        }
                    },
                    t => children.push(next_datum(port, t)?),
                }
            }
        },
        Some(Token::Vector) => {
            let mut children = Vec::new();
            loop {
                match next_token(port).map_err(ParseError::IoError)? {
                    Some(Token::Close) => return Ok(SyntaxObject::compound(Datum::Vector(Vec::new()), children, source_location)),
                    t => children.push(next_datum(port, t)?),
                }
            }
        },
        Some(Token::Bytes) => {
            let mut bytes = Vec::new();
            loop {
                match next_token(port).map_err(ParseError::IoError)? {
                    Some(Token::Close) => return Ok(SyntaxObject::simple(Datum::Bytes(bytes), source_location)),
                    Some(Token::Number(s, TokenTag::Integer)) => {
                        match s.parse::<u8>() {
                            Ok(b) => bytes.push(b),
                            Err(_) => return Err(ParseError::InvalidNumber("expected byte")),
                        }
                    },
                    _ => return Err(ParseError::UnexpectedToken),
                }
            }
        },
        Some(Token::Quote) => Ok(SyntaxObject::compound(Datum::List(Vec::new(), false),
            vec![SyntaxObject::simple(Datum::Symbol("quote".to_string()), source_location), next_datum(port, None)?],
            source_location)),
        Some(Token::Backquote) => Ok(SyntaxObject::compound(Datum::List(Vec::new(), false),
            vec![SyntaxObject::simple(Datum::Symbol("quasiquote".to_string()), source_location), next_datum(port, None)?],
            source_location)),
        Some(Token::Unquote) => Ok(SyntaxObject::compound(Datum::List(Vec::new(), false),
            vec![SyntaxObject::simple(Datum::Symbol("unquote".to_string()), source_location), next_datum(port, None)?],
            source_location)),
        Some(Token::UnquoteSplicing) => Ok(SyntaxObject::compound(Datum::List(Vec::new(), false),
            vec![SyntaxObject::simple(Datum::Symbol("unquote-splicing".to_string()), source_location), next_datum(port, None)?],
            source_location)),
        Some(_) => Err(ParseError::UnexpectedToken),
        None => Err(ParseError::NoToken),
    }
}
fn parse_boolean(s: &str) -> Result<bool, ()> {
    match s {
        "#t" | "#true" => Ok(true),
        "#f" | "#false" => Ok(false),
        _ => Err(()),
    }
}
fn parse_number(fs: String, st: TokenTag) -> Result<Datum, ParseError> {
    let mut s = fs.as_str();
    let mut radix = 10;
    let mut sign = 1;
    if s.starts_with("#b") {
        radix = 2;
        s = &s[2..];
    } else if s.starts_with("#o") {
        radix = 8;
        s = &s[2..];
    } else if s.starts_with("#x") {
        radix = 16;
        s = &s[2..];
    } else if s.starts_with("#d") {
        radix = 10;
        s = &s[2..];
    }

    if s.starts_with('+') {
        sign = 1;
        s = &s[1..];
    } else if s.starts_with('-') {
        sign = -1;
        s = &s[1..];
    }

    match st {
        TokenTag::Integer => {
            match i64::from_str_radix(s, radix) {
                Ok(n) => Ok(Datum::Integer(sign * n)),
                Err(e) => match e.kind() {
                    std::num::IntErrorKind::Zero => Ok(Datum::Integer(0)),
                    std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                        match s.parse::<f64>() {
                            Ok(f) => Ok(Datum::Float(sign as f64 * f)),
                            Err(_) => Err(ParseError::InvalidNumber("illegal number")),
                        }
                    },
                    _ => Err(ParseError::InvalidNumber("illegal number")),
                }
            }
        }
        TokenTag::Rational => {
            let mut parts = s.splitn(2, '/');
            if let (Some(nstr), Some(dstr)) = (parts.next(), parts.next()) {
                match (i64::from_str_radix(nstr.trim(), radix), i64::from_str_radix(dstr.trim(), radix)) {
                    (Ok(n), Ok(d)) => {
                        if d == 0 {
                            Err(ParseError::InvalidNumber("division by zero"))
                        } else {
                            Ok(Datum::Rational(sign * n, d))
                        }
                    },
                    _ => Err(ParseError::InvalidNumber("illegal number")),
                }
            } else {
                Err(ParseError::InvalidNumber("illegal number"))
            }
        }
        TokenTag::Decimal => match s.parse::<f64>() {
            Ok(f) => Ok(Datum::Float(sign as f64 * f)),
            Err(_) => Err(ParseError::InvalidNumber("illegal number")),
        },
        TokenTag::Infinity => Ok(Datum::Float(sign as f64 * f64::INFINITY)),
        TokenTag::NaN => Ok(Datum::Float(sign as f64 * f64::NAN)),
    }
}
fn parse_character(s: String) -> Result<char, ()> {
    match s.as_str() {
        "#\\alarm" => Ok('\u{0007}'),
        "#\\backspace" => Ok('\u{0008}'),
        "#\\delete" => Ok('\u{007F}'),
        "#\\escape" => Ok('\u{001B}'),
        "#\\newline" => Ok('\n'),
        "#\\return" => Ok('\r'),
        "#\\space" => Ok(' '),
        "#\\tab" => Ok('\t'),
        _ => {
            let mut cit = s.chars().skip(2);
            match cit.next().unwrap() {
                'x' => {
                    u32::from_str_radix(&s[3..], 16)
                        .ok()
                        .and_then(char::from_u32)
                        .ok_or(())
                },
                c => Ok(c),
            }
        }
    }
}
fn parse_string(rstr: String) -> Result<String, ()> {
    let mut ci = rstr.chars().skip(1).peekable();
    let mut s = String::with_capacity(rstr.len() - 2);
    while let Some(c) = ci.next() {
        match c {
            '"' => return Ok(s),
            '\\' => {
                match ci.next().ok_or(())? {
                    'a' => s.push('\u{0007}'),
                    'b' => s.push('\u{0008}'),
                    'n' => s.push('\n'),
                    'r' => s.push('\r'),
                    't' => s.push('\t'),
                    '"' => s.push('"'),
                    '\\' => s.push('\\'),
                    'x' => {
                        let mut ccode = 0;
                        while let Some(digit) = ci.next_if(|c| c.is_digit(16)) {
                            ccode = ccode * 16 + digit.to_digit(16).unwrap();
                        }
                        if ci.next().unwrap() == ';' {
                            s.push(unsafe { char::from_u32_unchecked(ccode)});
                        }
                    },
                    ' ' | '\t' | '\n' | '\r' => {
                        while let Some(c) = ci.next() {
                            if c != ' ' && c != '\t' {
                                break;
                            }
                        }
                        match ci.next().ok_or(())? {
                            '\n' => s.push('\n'),
                            '\r' => {
                                s.push('\r');
                                if ci.peek().is_some_and(|&c| c == '\n') {
                                    s.push('\n');
                                    ci.next();
                                }
                            },
                            _ => return Err(()),
                        }
                        while let Some(c) = ci.next() {
                            if c != ' ' && c != '\t' {
                                break;
                            }
                        }
                    },
                    _ => return Err(()),
                };
            },
            _ => s.push(c),
        }
    }
    Err(())
}
fn parse_identifier(rstr: String) -> Result<String, ()> {
    if rstr.starts_with('|') {
        let mut ci = rstr.chars().skip(1).peekable();
        let mut s = String::new();
        while let Some(c) = ci.next() {
            match c {
                '|' => return Ok(s),
                '\\' => {
                    match ci.next().ok_or(())? {
                        'a' => s.push('\u{0007}'),
                        'b' => s.push('\u{0008}'),
                        'n' => s.push('\n'),
                        'r' => s.push('\r'),
                        't' => s.push('\t'),
                        '|' => s.push('|'),
                        '\\' => s.push('\\'),
                        'x' => {
                            let mut ccode = 0;
                            while let Some(digit) = ci.next_if(|c| c.is_digit(16)) {
                                ccode = ccode * 16 + digit.to_digit(16).unwrap();
                            }
                            if ci.next().unwrap() == ';' {
                                s.push(unsafe { char::from_u32_unchecked(ccode)});
                            }
                        },
                        _ => return Err(()),
                    };
                },
                _ => s.push(c),
            }
        }
        Err(())
    } else {
        Ok(rstr)
    }
}

fn next_token(port: &mut InputPort) -> Result<Option<Token>, std::io::Error> {
    while let Some(dh) = atmosphere(port, 0)? {
        port.advance(dh);
    }

    if let Some(r) = boolean(port, 0)? {
        if r > 0 {
            return Ok(Some(Token::Boolean(parse_boolean(port.advance(r)).unwrap())));
        }
    }
    if let Some((r, st)) = number(port, 0)? {
        if r > 0 {
            return Ok(Some(Token::Number(port.advance(r).to_string(), st)));
        }
    }
    if let Some(r) = character(port, 0)? {
        if r > 0 {
            return Ok(Some(Token::Character(port.advance(r).to_string())));
        }
    }
    if let Some(r) = string(port, 0)? {
        if r > 0 {
            return Ok(Some(Token::String(port.advance(r).to_string())));
        }
    }
    if let Some(r) = identifier(port, 0)? {
        if r > 0 {
            return Ok(Some(Token::Identifier(port.advance(r).to_string())));
        }
    }
    if port.test("(", 0)? {
        port.advance(1);
        return Ok(Some(Token::Open));
    }
    if port.test(")", 0)? {
        port.advance(1);
        return Ok(Some(Token::Close));
    }
    if port.test("#(", 0)? {
        port.advance(2);
        return Ok(Some(Token::Vector));
    }
    if port.test("#u8(", 0)? {
        port.advance(4);
        return Ok(Some(Token::Bytes));
    }
    if port.test("'", 0)? {
        port.advance(1);
        return Ok(Some(Token::Quote));
    }
    if port.test("`", 0)? {
        port.advance(1);
        return Ok(Some(Token::Backquote));
    }
    if port.test(",@", 0)? {
        port.advance(2);
        return Ok(Some(Token::UnquoteSplicing));
    }
    if port.test(",", 0)? {
        port.advance(1);
        return Ok(Some(Token::Unquote));
    }
    if port.test(".", 0)? {
        if let Some(_) = delimiter(port, 1)? {
            port.advance(1);
            return Ok(Some(Token::Dot));
        }
    }

    Ok(None)
}
fn delimiter(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = whitespace(port, offset)? {
        Ok(Some(r))
    } else {
        match port.peek(offset)? {
            Some(b'(') | Some(b')') | Some(b'"') | Some(b';') | Some(b'|') => Ok(Some(1)),
            _ => Ok(None),
        }
    }
}
fn intraline_whitespace(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if port.test(" ", offset)? || port.test("\t", offset)? {
        Ok(Some(1))
    } else {
        Ok(None)
    }
}
fn whitespace(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = intraline_whitespace(port, offset)? {
        Ok(Some(r))
    } else if let Some(r) = line_ending(port, offset)? {
        Ok(Some(r))
    } else {
        Ok(None)
    }
}
fn line_ending(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if port.test("\n", offset)? {
        Ok(Some(1))
    } else if port.test("\r\n", offset)? {
        Ok(Some(2))
    } else if port.test("\r", offset)? {
        Ok(Some(1))
    } else {
        Ok(None)
    }
}
fn comment(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = nested_comment(port, offset)? {
        Ok(Some(r))
    } else if port.peek(offset)? == Some(b';') {
        let mut r = 1;
        while line_ending(port, offset + r)?.is_none() {
            r += 1;
        }
        Ok(Some(r))
    } else {
        Ok(None)
    }
}
fn nested_comment(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if port.test("#|", offset)? {
        let mut r = 2 + comment_text(port, offset + 2)?.unwrap_or(0);
        while let Some(dr) = comment_cont(port, offset + r)? {
            r += dr;
            if dr == 0 {
                break;
            }
        }
        if port.test("|#", offset + r)? {
            return Ok(Some(r + 2));
        }
    }
    Ok(None)
}
fn comment_text(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    let mut r = 0;
    while !port.test("#|", offset + r)? && !port.test("|#", offset + r)? {
        r += 1;
    }
    Ok(Some(r))
}
fn comment_cont(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = nested_comment(port, offset)? {
        Ok(Some(r + comment_text(port, offset + r)?.unwrap_or(0)))
    } else {
        Ok(None)
    }
}
fn atmosphere(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = comment(port, offset)? {
        Ok(Some(r))
    } else if let Some(r) = whitespace(port, offset)? {
        Ok(Some(r))
    } else {
        Ok(None)
    }
}
fn identifier(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(mut r) = initial(port, offset)? {
        while let Some(dr) = subsequent(port, offset + r)? {
            r += dr;
        }
        if let Some(_) = delimiter(port, offset + r)? {
            return Ok(Some(r));
        }
    }
    if let Some(r) = peculiar_identifier(port, offset)? {
        return Ok(Some(r));
    }
    if port.test("|", offset)? {
        let mut r = 1;
        while let Some(dr) = symbol_element(port, offset + r)? {
            r += dr;
        }
        if port.test("|", offset + r)? {
            return Ok(Some(r + 1));
        }
    }
    Ok(None)
}
fn initial(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    match port.peek(offset)? {
        Some(b'a'..=b'z') | Some(b'A'..=b'Z') => Ok(Some(1)),
        Some(b'!') | Some(b'$') | Some(b'%') | Some(b'&') => Ok(Some(1)),
        Some(b'*') | Some(b'/') | Some(b':') | Some(b'<') => Ok(Some(1)),
        Some(b'=') | Some(b'>') | Some(b'?') | Some(b'^') => Ok(Some(1)),
        Some(b'_') | Some(b'~') => Ok(Some(1)),
        _ => Ok(None),
    }
}
fn subsequent(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = initial(port, offset)? {
        return Ok(Some(r));
    }
    if let Some(r) = digit(port, offset, 10)? {
        return Ok(Some(r));
    }
    match port.peek(offset)? {
        Some(b'+') | Some(b'-') | Some(b'.') | Some(b'@') => Ok(Some(1)),
        _ => Ok(None),
    }
}
fn digit(port: &mut InputPort, offset: usize, base: i32) -> Result<Option<usize>, std::io::Error> {
    match port.peek(offset)? {
        Some(b'0'..=b'1') if base >= 2 => Ok(Some(1)),
        Some(b'0'..=b'7') if base >= 8 => Ok(Some(1)),
        Some(b'0'..=b'9') if base >= 10 => Ok(Some(1)),
        Some(b'a'..=b'f') if base >= 16 => Ok(Some(1)),
        Some(b'A'..=b'F') if base >= 16 => Ok(Some(1)),
        _ => Ok(None),
    }
}
fn sign(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    match port.peek(offset)? {
        Some(b'+') | Some(b'-') => Ok(Some(1)),
        _ => Ok(Some(0)),
    }
}
fn inline_hex_escape(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if port.test("\\x", offset)? {
        let mut r = 2;
        while let Some(dr) = digit(port, offset + r, 16)? {
            r += dr;
        }
        if port.test(";", offset + r)? {
            return Ok(Some(r + 1));
        }
    }
    Ok(None)
}
fn mnemonic_escape(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    match port.peek(offset)? {
        Some(b'\\') => match port.peek(offset + 1)? {
            Some(b'a') => Ok(Some(1)),
            Some(b'b') => Ok(Some(1)),
            Some(b'n') => Ok(Some(1)),
            Some(b'r') => Ok(Some(1)),
            Some(b't') => Ok(Some(1)),
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}
fn peculiar_identifier(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    let r = sign(port, offset)?.unwrap();
    if port.peek(offset + r)? == Some(b'.') {
        if let Some(mut s) = dot_subsequent(port, offset + r + 1)? {
            while let Some(ds) = subsequent(port, offset + r + 1 + s)? {
                s += ds;
            }
            return Ok(Some(r + 1 + s));
        }
    }
    if r > 0 {
        if let Some(mut s) = sign_subsequent(port, offset + r)? {
            while let Some(ds) = subsequent(port, offset + r + s)? {
                s += ds;
            }
            return Ok(Some(r + s));
        } else {
            return Ok(Some(r));
        }
    }
    Ok(None)
}
fn dot_subsequent(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = sign_subsequent(port, offset)? {
        Ok(Some(r))
    } else if port.peek(offset)? == Some(b'.') {
        Ok(Some(1))
    } else {
        Ok(None)
    }
}
fn sign_subsequent(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = initial(port, offset)? {
        return Ok(Some(r));
    }
    match port.peek(offset)? {
        Some(b'+') | Some(b'-') | Some(b'@') => Ok(Some(1)),
        _ => Ok(None),
    }
}
fn symbol_element(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = inline_hex_escape(port, offset)? {
        Ok(Some(r))
    } else if let Some(r) = mnemonic_escape(port, offset)? {
        Ok(Some(r))
    } else {
        match port.peek_char(offset)? {
            Some('|') => Ok(None),
            Some('\\') => {
                match port.peek(offset + 1)? {
                    Some(b'|') => Ok(Some(2)),
                    _ => Ok(None),
                }
            },
            Some(c) => Ok(Some(c.len_utf8())),
            None => Ok(None),
        }
    }
}
fn boolean(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    match if port.test("#true", offset)? {
        Some(5)
    } else if port.test("#false", offset)? {
        Some(6)
    } else if port.test("#t", offset)? || port.test("#f", offset)? {
        Some(2)
    } else {
        None
    }
    {
        Some(r) => {
            if let Some(_) = delimiter(port, offset + r)? {
                Ok(Some(r))
            } else {
                Ok(None)
            }
        },
        None => Ok(None),
    }
}
fn character(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if port.test("#\\", offset)? {
        let r = if port.test("alarm", offset + 2)? {
            Some(7)
        } else if port.test("backspace", offset + 2)? {
            Some(11)
        } else if port.test("delete", offset + 2)? {
            Some(8)
        } else if port.test("escape", offset + 2)? {
            Some(8)
        } else if port.test("newline", offset + 2)? {
            Some(9)
        } else if port.test("return", offset + 2)? {
            Some(8)
        } else if port.test("space", offset + 2)? {
            Some(7)
        } else if port.test("tab", offset + 2)? {
            Some(7)
        } else if port.test("x", offset + 2)? {
            let mut r = 3;
            while let Some(dr) = digit(port, offset + r, 16)? {
                r += dr;
            }
            Some(r)
        } else {
            port.peek_char(offset + 2)?.map(|c| 2 + c.len_utf8())
        };

        if r.is_some_and(|r| r > 0) {
            if delimiter(port, offset + r.unwrap())?.is_some() {
                return Ok(r);
            }
        }
    }
    Ok(None)
}
fn string(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if port.peek(offset)? == Some(b'"'){
        let mut r = 1;
        while let Some(dr) = string_element(port, offset + r)? {
            r += dr;
        }
        if port.peek(offset + r)? == Some(b'"') {
            return Ok(Some(r + 1));
        }
    }
    Ok(None)
}
fn string_element(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = inline_hex_escape(port, offset)? {
        return Ok(Some(r));
    }
    if let Some(r) = mnemonic_escape(port, offset)? {
        return Ok(Some(r));
    }
    if let Some(c) = port.peek_char(offset)? {
        if c == '\\' {
            let nc = port.peek(offset + 1)?;
            if nc == Some(b'"') || nc == Some(b'\\') {
                return Ok(Some(2));
            }

            let mut r = 1;
            while let Some(dr) = intraline_whitespace(port, offset + r)? {
                r += dr;
            }
            if let Some(dr) = line_ending(port, offset + r)? {
                r += dr;
                while let Some(dr) = intraline_whitespace(port, offset + r)? {
                    r += dr;
                }
                return Ok(Some(r));
            }
        } else if c != '"' {
            return Ok(Some(c.len_utf8()));
        }
    }
    Ok(None)
}
fn number(port: &mut InputPort, offset: usize) -> Result<Option<(usize, TokenTag)>, std::io::Error> {
    for base in [2, 8, 10, 16] {
        if let Some(r) = radix(port, offset, base)? {
            if let Some((s, st)) = real(port, offset + r, base)? {
                if let Some(_) = delimiter(port, offset + r + s)? {
                    return Ok(Some((r + s, st)));
                }
            }
        }
    }
    Ok(None)
}
fn real(port: &mut InputPort, offset: usize, base: i32) -> Result<Option<(usize, TokenTag)>, std::io::Error> {
    if let Some((r, st)) = infnan(port, offset)? {
        return Ok(Some((r, st)));
    }
    if let Some(r) = sign(port, offset)? {
        if let Some((s, st)) = ureal(port, offset + r, base)? {
            return Ok(Some((r + s, st)));
        }
    }
    Ok(None)
}
fn ureal(port: &mut InputPort, offset: usize, base: i32) -> Result<Option<(usize, TokenTag)>, std::io::Error> {
    if let Some(r) = decimal(port, offset)? {
        if base == 10 {
            return Ok(Some((r, TokenTag::Decimal)));
        }
    }
    if let Some(r) = uinteger(port, offset, base)? {
        if port.peek(offset + r)? == Some(b'/') {
            if let Some(s) = uinteger(port, offset + r + 1, base)? {
                return Ok(Some((r + s + 1, TokenTag::Rational)));
            }
        } else {
            return Ok(Some((r, TokenTag::Integer)));
        }
    }
    Ok(None)
}
fn decimal(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(mut r) = uinteger(port, offset, 10)? {
        if port.peek(offset + r)? == Some(b'.') {
            r += 1;
            while let Some(dr) = digit(port, offset + r, 10)? {
                r += dr;
            }
            if let Some(s) = suffix(port, offset + r)? {
                return Ok(Some(r + s));
            }
        } else if let Some(s) = suffix(port, offset + r)? {
            if s > 0 {
                return Ok(Some(r + s));
            }
        }
    } else if port.peek(offset)? == Some(b'.') {
        let mut r = 1;
        while let Some(dr) = digit(port, offset + r, 10)? {
            r += dr;
        }
        if r > 1 {
            if let Some(s) = suffix(port, offset + r)? {
                return Ok(Some(r + s));
            }
        }
    }
    Ok(None)
}
fn uinteger(port: &mut InputPort, offset: usize, base: i32) -> Result<Option<usize>, std::io::Error> {
    if let Some(mut r) = digit(port, offset, base)? {
        while let Some(dr) = digit(port, offset + r, base)? {
            r += dr;
        }
        Ok(Some(r))
    } else {
        Ok(None)
    }
}
fn infnan(port: &mut InputPort, offset: usize) -> Result<Option<(usize, TokenTag)>, std::io::Error> {
    if port.test("+inf.0", offset)? || port.test("-inf.0", offset)? {
        Ok(Some((6, TokenTag::Infinity)))
    } else if port.test("+nan.0", offset)? || port.test("-nan.0", offset)? {
        Ok(Some((6, TokenTag::NaN)))
    } else {
        Ok(None)
    }
}
fn suffix(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    match port.peek(offset)? {
        Some(b'e') | Some(b'E') => {
            if let Some(r) = sign(port, offset + 1)? {
                if let Some(s) = uinteger(port, offset + r + 1, 10)? {
                    return Ok(Some(r + s + 1));
                }
            }
            Ok(None)
        },
        _ => Ok(Some(0)),
    }
}
fn radix(port: &mut InputPort, offset: usize, base: i32) -> Result<Option<usize>, std::io::Error> {
    if base == 2 && port.test("#b", offset)? {
        Ok(Some(2))
    } else if base == 8 && port.test("#b", offset)? {
        Ok(Some(2))
    } else if base == 10 {
        if port.test("#d", offset)? {
            Ok(Some(2))
        } else {
            Ok(Some(0))
        }
    } else if base == 16 && port.test("#x", offset)? {
        Ok(Some(2))
    } else {
        Ok(None)
    }
}

/******** macro expansion *********/
#[derive(Debug, Clone)]
pub enum ExpansionError {
}

impl SyntaxObject {
    pub fn expand(self) -> Result<Datum, ExpansionError> {
        todo!()
    }
}
