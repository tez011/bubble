use std::borrow::Cow;

pub struct InputPort<'a, 'b> {
    reader: Option<&'a mut dyn std::io::Read>,
    buffer: Cow<'b, [u8]>,
    head: usize,
    tail: usize,
    source: (u32, u32),
}
impl<'a, 'b> TryFrom<&'a mut dyn std::io::Read> for InputPort<'a, 'b> {
    type Error = std::io::Error;

    fn try_from(reader: &'a mut dyn std::io::Read) -> Result<Self, Self::Error> {
        let mut buffer = vec![0u8; 4096];
        let tail = reader.read(&mut buffer)?;
        Ok(InputPort {
            reader: Some(reader),
            buffer: Cow::Owned(buffer),
            head: 0,
            tail,
            source: (1, 0),
        })
    }
}
impl<'a, 'b> From<&'b [u8]> for InputPort<'a, 'b> {
    fn from(buffer: &'b [u8]) -> Self {
        InputPort {
            reader: None,
            buffer: Cow::Borrowed(buffer),
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
    pub fn location(&self) -> (u32, u32) {
        self.source
    }
    pub fn advance(&mut self, r: usize) -> &str {
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

    fn expand(&mut self) {
        if self.reader.is_some() {
            let mut buffer = vec![0u8; self.buffer.len() * 2];
            let edata = &self.buffer[self.head..self.tail];
            buffer[..edata.len()].copy_from_slice(edata);
            self.head = 0;
            self.tail = edata.len();
            self.buffer = Cow::Owned(buffer);
        }
    }
}
