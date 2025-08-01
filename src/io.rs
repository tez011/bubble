use std::char;

pub struct InputPort<'a, 'b> {
    reader: Option<&'a mut dyn std::io::Read>,
    buffer: std::borrow::Cow<'b, [u8]>,
    head: usize,
    tail: usize,
    source: (usize, usize),
}
impl<'a, 'b> TryFrom<&'a mut dyn std::io::Read> for InputPort<'a, 'b> {
    type Error = std::io::Error;

    fn try_from(reader: &'a mut dyn std::io::Read) -> Result<Self, Self::Error> {
        let mut buffer = vec![0u8; 4096];
        let tail = reader.read(&mut buffer)?;
        Ok(InputPort {
            reader: Some(reader),
            buffer: std::borrow::Cow::Owned(buffer),
            head: 0,
            tail,
            source: (0, 0),
        })
    }
}
impl<'a, 'b> From<&'b [u8]> for InputPort<'a, 'b> {
    fn from(buffer: &'b [u8]) -> Self {
        InputPort {
            reader: None,
            buffer: std::borrow::Cow::Borrowed(buffer),
            head: 0,
            tail: buffer.len(),
            source: (0, 0),
        }
    }
}
impl<'a, 'b> InputPort<'a, 'b> {
    pub fn slurp(&mut self) -> Result<usize, std::io::Error> {
        if self.reader.is_none() {
            return Ok(0);
        }

        if self.head > 0 {
            self.buffer.to_mut().copy_within(self.head..self.tail, 0);
            self.tail -= self.head;
            if self.head > self.buffer.len() / 2 {
                self.expand();
            }
            self.head = 0;
        }

        let r = self.reader.as_mut().unwrap().read(&mut self.buffer.to_mut()[self.tail..])?;
        self.tail += r;
        Ok(r)
    }
    pub fn test<T: ?Sized + AsRef<[u8]>>(&mut self, s: &T, offset: usize) -> Result<bool, std::io::Error> {
        let s = s.as_ref();
        let mut sit = s.iter();
        let mut bit = self.buffer[self.head + offset..self.tail].iter();
        loop {
            match (sit.next(), bit.next()) {
                (Some(sb), Some(bb)) => if sb != bb {
                    return Ok(false);
                },
                (None, _) => return Ok(true),
                (Some(_), None) => {
                    if self.slurp()? > 0 {
                        return self.test(s, offset);
                    } else {
                        return Ok(false);
                    }
                }
            }
        }
    }
    pub fn peek(&mut self, offset: usize) -> Result<Option<u8>, std::io::Error> {
        let mut slice = &self.buffer[self.head + offset..self.tail];
        if slice.is_empty() {
            self.slurp()?;
            slice = &self.buffer[self.head + offset..self.tail];
        }
        if let Some(&b) = slice.first() {
            Ok(Some(b))
        } else {
            Ok(None)
        }
    }
    pub fn peek_char(&mut self, offset: usize) -> Result<Option<char>, std::io::Error> {
        let mut slice = std::str::from_utf8(&self.buffer[self.head + offset..self.tail]);
        if slice.is_err() || slice.as_ref().unwrap().is_empty() {
            self.slurp()?;
            slice = std::str::from_utf8(&self.buffer[self.head + offset..self.tail]);
        }

        if let Some(c) = slice.unwrap().chars().next() {
            Ok(Some(c))
        } else {
            Ok(None)
        }
    }
    pub fn location(&self) -> (usize, usize) {
        self.source
    }

    fn expand(&mut self) {
        if self.reader.is_some() {
            let mut buffer = vec![0u8; self.buffer.len() * 2];
            let edata = &self.buffer[self.head..self.tail];
            buffer[..edata.len()].copy_from_slice(edata);
            self.head = 0;
            self.tail = edata.len();
            self.buffer = std::borrow::Cow::Owned(buffer);
        }
    }
    fn advance(&mut self, r: usize) -> &str {
        let slice = unsafe { std::str::from_utf8_unchecked(&self.buffer[self.head..self.head + r]) };
        let mut cit = slice.bytes().peekable();
        while let Some(c) = cit.next() {
            match c {
                b'\n' => {
                    self.source.0 += 1;
                    self.source.1 = 0;
                },
                b'\r' => {
                    self.source.0 += 1;
                    self.source.1 = 0;
                    cit.next_if(|&c| c == b'\n');
                },
                _ => self.source.1 += 1,
            }
        }
        self.head += r;
        slice
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

#[derive(Debug, Clone)]
pub enum Datum {
    Boolean(bool),
    Integer(i64),
    Rational(i64, i64),
    Float(f64),
    Character(char),
    String(String),
    Symbol(String),
    Bytes(Vec<u8>),
    List(Vec<Datum>),
    ImproperList(Vec<Datum>),
    Vector(Vec<Datum>),
    Quoted(Box<Datum>),
    Backquoted(Box<Datum>),
    Unquoted(Box<Datum>),
    UnquotedInline(Box<Datum>),
}
#[derive(Debug)]
pub enum ParseError {
    NoToken,
    UnexpectedToken,
    IoError(std::io::Error),
    InvalidNumber(&'static str),
    InvalidCharacter,
}

pub struct Parser<'a, 'b, 'c> {
    port: &'c mut InputPort<'a, 'b>,
}
impl<'a, 'b, 'c> Parser<'a, 'b, 'c> {
    pub fn next(port: &'c mut InputPort<'a, 'b>) -> Result<Datum, ParseError> {
        Parser { port }.next_datum(None)
    }

    fn next_datum(&mut self, next_token: Option<Token>) -> Result<Datum, ParseError> {
        let next_token = match next_token {
            Some(t) => Some(t),
            None => self.next_token().map_err(ParseError::IoError)?
        };
        match next_token {
            Some(Token::Boolean(b)) => Ok(Datum::Boolean(b)),
            Some(Token::Number(s, st)) => Self::parse_number(s, st),
            Some(Token::Character(s)) => Ok(Datum::Character(Self::parse_character(s).map_err(|_| ParseError::InvalidCharacter)?)),
            Some(Token::String(s)) => Ok(Datum::String(Self::parse_string(s).map_err(|_| ParseError::InvalidCharacter)?)),
            Some(Token::Identifier(s)) => Ok(Datum::Symbol(Self::parse_identifier(s).map_err(|_| ParseError::InvalidCharacter)?)),
            Some(Token::Open) => {
                let mut children = Vec::new();
                loop {
                    match self.next_token().map_err(ParseError::IoError)? {
                        Some(Token::Close) => return Ok(Datum::List(children)),
                        Some(Token::Dot) => {
                            children.push(self.next_datum(None)?);
                            match self.next_token().map_err(ParseError::IoError)? {
                                Some(Token::Close) => return Ok(Datum::ImproperList(children)),
                                t => return Err(ParseError::UnexpectedToken),
                            }
                        },
                        t => children.push(self.next_datum(t)?),
                    }
                }
            },
            Some(Token::Vector) => {
                let mut children = Vec::new();
                loop {
                    match self.next_token().map_err(ParseError::IoError)? {
                        Some(Token::Close) => return Ok(Datum::Vector(children)),
                        t => children.push(self.next_datum(t)?),
                    }
                }
            },
            Some(Token::Bytes) => {
                let mut bytes = Vec::new();
                loop {
                    match self.next_token().map_err(ParseError::IoError)? {
                        Some(Token::Close) => return Ok(Datum::Bytes(bytes)),
                        t => match self.next_datum(t)? {
                            Datum::Integer(b) => {
                                if 0 <= b && b < 256 {
                                    bytes.push(b as u8);
                                } else {
                                    return Err(ParseError::InvalidNumber("byte out of range"));
                                }
                            },
                            _ => return Err(ParseError::InvalidNumber("expected byte")),
                        }
                    }
                }
            },
            Some(Token::Quote) => Ok(Datum::Quoted(Box::new(self.next_datum(None)?))),
            Some(Token::Backquote) => Ok(Datum::Backquoted(Box::new(self.next_datum(None)?))),
            Some(Token::Unquote) => Ok(Datum::Unquoted(Box::new(self.next_datum(None)?))),
            Some(Token::UnquoteSplicing) => Ok(Datum::UnquotedInline(Box::new(self.next_datum(None)?))),
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

    fn next_token(&mut self) -> Result<Option<Token>, std::io::Error> {
        while let Some(dh) = self.atmosphere(0)? {
            self.port.advance(dh);
        }

        if let Some(r) = self.boolean(0)? {
            if r > 0 {
                return Ok(Some(Token::Boolean(Self::parse_boolean(self.port.advance(r)).unwrap())));
            }
        }
        if let Some((r, st)) = self.number(0)? {
            if r > 0 {
                return Ok(Some(Token::Number(self.port.advance(r).to_string(), st)));
            }
        }
        if let Some(r) = self.character(0)? {
            if r > 0 {
                return Ok(Some(Token::Character(self.port.advance(r).to_string())));
            }
        }
        if let Some(r) = self.string(0)? {
            if r > 0 {
                return Ok(Some(Token::String(self.port.advance(r).to_string())));
            }
        }
        if let Some(r) = self.identifier(0)? {
            if r > 0 {
                return Ok(Some(Token::Identifier(self.port.advance(r).to_string())));
            }
        }
        if self.port.test("(", 0)? {
            self.port.advance(1);
            return Ok(Some(Token::Open));
        }
        if self.port.test(")", 0)? {
            self.port.advance(1);
            return Ok(Some(Token::Close));
        }
        if self.port.test("#(", 0)? {
            self.port.advance(2);
            return Ok(Some(Token::Vector));
        }
        if self.port.test("#u8(", 0)? {
            self.port.advance(4);
            return Ok(Some(Token::Bytes));
        }
        if self.port.test("'", 0)? {
            self.port.advance(1);
            return Ok(Some(Token::Quote));
        }
        if self.port.test("`", 0)? {
            self.port.advance(1);
            return Ok(Some(Token::Backquote));
        }
        if self.port.test(",@", 0)? {
            self.port.advance(2);
            return Ok(Some(Token::UnquoteSplicing));
        }
        if self.port.test(",", 0)? {
            self.port.advance(1);
            return Ok(Some(Token::Unquote));
        }
        if self.port.test(".", 0)? {
            if let Some(_) = self.delimiter(1)? {
                self.port.advance(1);
                return Ok(Some(Token::Dot));
            }
        }

        Ok(None)
    }
    fn delimiter(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if let Some(r) = self.whitespace(offset)? {
            Ok(Some(r))
        } else {
            match self.port.peek(offset)? {
                Some(b'(') | Some(b')') | Some(b'"') | Some(b';') | Some(b'|') => Ok(Some(1)),
                _ => Ok(None),
            }
        }
    }
    fn intraline_whitespace(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if self.port.test(" ", offset)? || self.port.test("\t", offset)? {
            Ok(Some(1))
        } else {
            Ok(None)
        }
    }
    fn whitespace(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if let Some(r) = self.intraline_whitespace(offset)? {
            Ok(Some(r))
        } else if let Some(r) = self.line_ending(offset)? {
            Ok(Some(r))
        } else {
            Ok(None)
        }
    }
    fn line_ending(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if self.port.test("\n", offset)? {
            Ok(Some(1))
        } else if self.port.test("\r\n", offset)? {
            Ok(Some(2))
        } else if self.port.test("\r", offset)? {
            Ok(Some(1))
        } else {
            Ok(None)
        }
    }
    fn comment(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if let Some(r) = self.nested_comment(offset)? {
            Ok(Some(r))
        } else if self.port.peek(offset)? == Some(b';') {
            let mut r = 1;
            while self.line_ending(offset + r)?.is_none() {
                r += 1;
            }
            Ok(Some(r))
        } else {
            Ok(None)
        }
    }
    fn nested_comment(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if self.port.test("#|", offset)? {
            let mut r = 2 + self.comment_text(offset + 2)?.unwrap_or(0);
            while let Some(dr) = self.comment_cont(offset + r)? {
                r += dr;
                if dr == 0 {
                    break;
                }
            }
            if self.port.test("|#", offset + r)? {
                return Ok(Some(r + 2));
            }
        }
        Ok(None)
    }
    fn comment_text(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        let mut r = 0;
        while !self.port.test("#|", offset + r)? && !self.port.test("|#", offset + r)? {
            r += 1;
        }
        Ok(Some(r))
    }
    fn comment_cont(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if let Some(r) = self.nested_comment(offset)? {
            Ok(Some(r + self.comment_text(offset + r)?.unwrap_or(0)))
        } else {
            Ok(None)
        }
    }
    fn atmosphere(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if let Some(r) = self.comment(offset)? {
            Ok(Some(r))
        } else if let Some(r) = self.whitespace(offset)? {
            Ok(Some(r))
        } else {
            Ok(None)
        }
    }
    fn identifier(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if let Some(mut r) = self.initial(offset)? {
            while let Some(dr) = self.subsequent(offset + r)? {
                r += dr;
            }
            if let Some(_) = self.delimiter(offset + r)? {
                return Ok(Some(r));
            }
        }
        if let Some(r) = self.peculiar_identifier(offset)? {
            return Ok(Some(r));
        }
        if self.port.test("|", offset)? {
            let mut r = 1;
            while let Some(dr) = self.symbol_element(offset + r)? {
                r += dr;
            }
            if self.port.test("|", offset + r)? {
                return Ok(Some(r + 1));
            }
        }
        Ok(None)
    }
    fn initial(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        match self.port.peek(offset)? {
            Some(b'a'..=b'z') | Some(b'A'..=b'Z') => Ok(Some(1)),
            Some(b'!') | Some(b'$') | Some(b'%') | Some(b'&') => Ok(Some(1)),
            Some(b'*') | Some(b'/') | Some(b':') | Some(b'<') => Ok(Some(1)),
            Some(b'=') | Some(b'>') | Some(b'?') | Some(b'^') => Ok(Some(1)),
            Some(b'_') | Some(b'~') => Ok(Some(1)),
            _ => Ok(None),
        }
    }
    fn subsequent(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if let Some(r) = self.initial(offset)? {
            return Ok(Some(r));
        }
        if let Some(r) = self.digit(offset, 10)? {
            return Ok(Some(r));
        }
        match self.port.peek(offset)? {
            Some(b'+') | Some(b'-') | Some(b'.') | Some(b'@') => Ok(Some(1)),
            _ => Ok(None),
        }
    }
    fn digit(&mut self, offset: usize, base: i32) -> Result<Option<usize>, std::io::Error> {
        match self.port.peek(offset)? {
            Some(b'0'..=b'1') if base >= 2 => Ok(Some(1)),
            Some(b'0'..=b'7') if base >= 8 => Ok(Some(1)),
            Some(b'0'..=b'9') if base >= 10 => Ok(Some(1)),
            Some(b'a'..=b'f') if base >= 16 => Ok(Some(1)),
            Some(b'A'..=b'F') if base >= 16 => Ok(Some(1)),
            _ => Ok(None),
        }
    }
    fn sign(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        match self.port.peek(offset)? {
            Some(b'+') | Some(b'-') => Ok(Some(1)),
            _ => Ok(Some(0)),
        }
    }
    fn inline_hex_escape(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if self.port.test("\\x", offset)? {
            let mut r = 2;
            while let Some(dr) = self.digit(offset + r, 16)? {
                r += dr;
            }
            if self.port.test(";", offset + r)? {
                return Ok(Some(r + 1));
            }
        }
        Ok(None)
    }
    fn mnemonic_escape(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        match self.port.peek(offset)? {
            Some(b'\\') => match self.port.peek(offset + 1)? {
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
    fn peculiar_identifier(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        let r = self.sign(offset)?.unwrap();
        if self.port.peek(offset + r)? == Some(b'.') {
            if let Some(mut s) = self.dot_subsequent(offset + r + 1)? {
                while let Some(ds) = self.subsequent(offset + r + 1 + s)? {
                    s += ds;
                }
                return Ok(Some(r + 1 + s));
            }
        }
        if r > 0 {
            if let Some(mut s) = self.sign_subsequent(offset + r)? {
                while let Some(ds) = self.subsequent(offset + r + s)? {
                    s += ds;
                }
                return Ok(Some(r + s));
            } else {
                return Ok(Some(r));
            }
        }
        Ok(None)
    }
    fn dot_subsequent(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if let Some(r) = self.sign_subsequent(offset)? {
            Ok(Some(r))
        } else if self.port.peek(offset)? == Some(b'.') {
            Ok(Some(1))
        } else {
            Ok(None)
        }
    }
    fn sign_subsequent(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if let Some(r) = self.initial(offset)? {
            return Ok(Some(r));
        }
        match self.port.peek(offset)? {
            Some(b'+') | Some(b'-') | Some(b'@') => Ok(Some(1)),
            _ => Ok(None),
        }
    }
    fn symbol_element(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if let Some(r) = self.inline_hex_escape(offset)? {
            Ok(Some(r))
        } else if let Some(r) = self.mnemonic_escape(offset)? {
            Ok(Some(r))
        } else {
            match self.port.peek_char(offset)? {
                Some('|') => Ok(None),
                Some('\\') => {
                    match self.port.peek(offset + 1)? {
                        Some(b'|') => Ok(Some(2)),
                        _ => Ok(None),
                    }
                },
                Some(c) => Ok(Some(c.len_utf8())),
                None => Ok(None),
            }
        }
    }
    fn boolean(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        match if self.port.test("#true", offset)? {
            Some(5)
        } else if self.port.test("#false", offset)? {
            Some(6)
        } else if self.port.test("#t", offset)? || self.port.test("#f", offset)? {
            Some(2)
        } else {
            None
        }
        {
            Some(r) => {
                if let Some(_) = self.delimiter(offset + r)? {
                    Ok(Some(r))
                } else {
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }
    fn character(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if self.port.test("#\\", offset)? {
            let r = if self.port.test("alarm", offset + 2)? {
                Some(7)
            } else if self.port.test("backspace", offset + 2)? {
                Some(11)
            } else if self.port.test("delete", offset + 2)? {
                Some(8)
            } else if self.port.test("escape", offset + 2)? {
                Some(8)
            } else if self.port.test("newline", offset + 2)? {
                Some(9)
            } else if self.port.test("return", offset + 2)? {
                Some(8)
            } else if self.port.test("space", offset + 2)? {
                Some(7)
            } else if self.port.test("tab", offset + 2)? {
                Some(7)
            } else if self.port.test("x", offset + 2)? {
                let mut r = 3;
                while let Some(dr) = self.digit(offset + r, 16)? {
                    r += dr;
                }
                Some(r)
            } else {
                self.port.peek_char(offset + 2)?.map(|c| 2 + c.len_utf8())
            };

            if r.is_some_and(|r| r > 0) {
                if self.delimiter(offset + r.unwrap())?.is_some() {
                    return Ok(r);
                }
            }
        }
        Ok(None)
    }
    fn string(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if self.port.peek(offset)? == Some(b'"'){
            let mut r = 1;
            while let Some(dr) = self.string_element(offset + r)? {
                r += dr;
            }
            if self.port.peek(offset + r)? == Some(b'"') {
                return Ok(Some(r + 1));
            }
        }
        Ok(None)
    }
    fn string_element(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if let Some(r) = self.inline_hex_escape(offset)? {
            return Ok(Some(r));
        }
        if let Some(r) = self.mnemonic_escape(offset)? {
            return Ok(Some(r));
        }
        if let Some(c) = self.port.peek_char(offset)? {
            if c == '\\' {
                let nc = self.port.peek(offset + 1)?;
                if nc == Some(b'"') || nc == Some(b'\\') {
                    return Ok(Some(2));
                }

                let mut r = 1;
                while let Some(dr) = self.intraline_whitespace(offset + r)? {
                    r += dr;
                }
                if let Some(dr) = self.line_ending(offset + r)? {
                    r += dr;
                    while let Some(dr) = self.intraline_whitespace(offset + r)? {
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
    fn number(&mut self, offset: usize) -> Result<Option<(usize, TokenTag)>, std::io::Error> {
        for base in [2, 8, 10, 16] {
            if let Some(r) = self.radix(offset, base)? {
                if let Some((s, st)) = self.real(offset + r, base)? {
                    if let Some(_) = self.delimiter(offset + r + s)? {
                        return Ok(Some((r + s, st)));
                    }
                }
            }
        }
        Ok(None)
    }
    fn real(&mut self, offset: usize, base: i32) -> Result<Option<(usize, TokenTag)>, std::io::Error> {
        if let Some((r, st)) = self.infnan(offset)? {
            return Ok(Some((r, st)));
        }
        if let Some(r) = self.sign(offset)? {
            if let Some((s, st)) = self.ureal(offset + r, base)? {
                return Ok(Some((r + s, st)));
            }
        }
        Ok(None)
    }
    fn ureal(&mut self, offset: usize, base: i32) -> Result<Option<(usize, TokenTag)>, std::io::Error> {
        if let Some(r) = self.decimal(offset)? {
            if base == 10 {
                return Ok(Some((r, TokenTag::Decimal)));
            }
        }
        if let Some(r) = self.uinteger(offset, base)? {
            if self.port.peek(offset + r)? == Some(b'/') {
                if let Some(s) = self.uinteger(offset + r + 1, base)? {
                    return Ok(Some((r + s + 1, TokenTag::Rational)));
                }
            } else {
                return Ok(Some((r, TokenTag::Integer)));
            }
        }
        Ok(None)
    }
    fn decimal(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        if let Some(mut r) = self.uinteger(offset, 10)? {
            if self.port.peek(offset + r)? == Some(b'.') {
                r += 1;
                while let Some(dr) = self.digit(offset + r, 10)? {
                    r += dr;
                }
                if let Some(s) = self.suffix(offset + r)? {
                    return Ok(Some(r + s));
                }
            } else if let Some(s) = self.suffix(offset + r)? {
                if s > 0 {
                    return Ok(Some(r + s));
                }
            }
        } else if self.port.peek(offset)? == Some(b'.') {
            let mut r = 1;
            while let Some(dr) = self.digit(offset + r, 10)? {
                r += dr;
            }
            if r > 1 {
                if let Some(s) = self.suffix(offset + r)? {
                    return Ok(Some(r + s));
                }
            }
        }
        Ok(None)
    }
    fn uinteger(&mut self, offset: usize, base: i32) -> Result<Option<usize>, std::io::Error> {
        if let Some(mut r) = self.digit(offset, base)? {
            while let Some(dr) = self.digit(offset + r, base)? {
                r += dr;
            }
            Ok(Some(r))
        } else {
            Ok(None)
        }
    }
    fn infnan(&mut self, offset: usize) -> Result<Option<(usize, TokenTag)>, std::io::Error> {
        if self.port.test("+inf.0", offset)? || self.port.test("-inf.0", offset)? {
            Ok(Some((6, TokenTag::Infinity)))
        } else if self.port.test("+nan.0", offset)? || self.port.test("-nan.0", offset)? {
            Ok(Some((6, TokenTag::NaN)))
        } else {
            Ok(None)
        }
    }
    fn suffix(&mut self, offset: usize) -> Result<Option<usize>, std::io::Error> {
        match self.port.peek(offset)? {
            Some(b'e') | Some(b'E') => {
                if let Some(r) = self.sign(offset + 1)? {
                    if let Some(s) = self.uinteger(offset + r + 1, 10)? {
                        return Ok(Some(r + s + 1));
                    }
                }
                Ok(None)
            },
            _ => Ok(Some(0)),
        }
    }
    fn radix(&mut self, offset: usize, base: i32) -> Result<Option<usize>, std::io::Error> {
        if base == 2 && self.port.test("#b", offset)? {
            Ok(Some(2))
        } else if base == 8 && self.port.test("#b", offset)? {
            Ok(Some(2))
        } else if base == 10 {
            if self.port.test("#d", offset)? {
                Ok(Some(2))
            } else {
                Ok(Some(0))
            }
        } else if base == 16 && self.port.test("#x", offset)? {
            Ok(Some(2))
        } else {
            Ok(None)
        }
    }
}
