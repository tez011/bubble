mod io;
mod syntax;

#[derive(Debug, Clone)]
pub enum Expression {
    Boolean(bool),
    Integer(i64),
    Rational(i64, i64),
    Float(f64),
    Character(char),
    String(String),
    Symbol(String),
    Bytes(Vec<u8>),
    List(Vec<Expression>),
    ImproperList(Vec<Expression>, Box<Expression>),
    Vector(Vec<Expression>),
}
impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Boolean(true) => write!(f, "#t"),
            Expression::Boolean(false) => write!(f, "#f"),
            Expression::Integer(i) => write!(f, "{}", i),
            Expression::Rational(n, d) => write!(f, "{}/{}", n, d),
            Expression::Float(fl) => write!(f, "{}", fl),
            Expression::Character(c) => write!(f, "\\#{}", c),
            Expression::String(s) => write!(f, "\"{}\"", s),
            Expression::Bytes(b) => write!(f, "{:?}", b),
            Expression::Symbol(s) => write!(f, "{}", s),
            Expression::List(children) => {
                write!(f, "(")?;
                for (i, child) in children.iter().enumerate() {
                    write!(f, "{}", child)?;
                    if i < children.len() - 1 {
                        write!(f, " ")?;
                    }
                }
                write!(f, ")")
            },
            Expression::ImproperList(children, tail) => {
                write!(f, "(")?;
                for (i, child) in children.iter().enumerate() {
                    write!(f, "{}", child)?;
                    if i < children.len() - 1 {
                        write!(f, " ")?;
                    }
                }
                write!(f, " . {})", tail)
            },
            Expression::Vector(children) => {
                write!(f, "(")?;
                for (i, child) in children.iter().enumerate() {
                    write!(f, "{}", child)?;
                    if i < children.len() - 1 {
                        write!(f, " ")?;
                    }
                }
                write!(f, ")")
            },
        }
    }
}

fn main() {
    let mut stdin = std::io::stdin().lock();
    let mut handle = io::InputPort::try_from(&mut stdin as &mut dyn std::io::Read).unwrap();
    let mut eenv = syntax::Environment::new();
    loop {
        let ns = syntax::read(&mut handle);
        if ns.is_err() {
            let location = handle.location();
            eprintln!("Parse error: {}:{}: {}", location.0, location.1, ns.unwrap_err());
            break;
        }

        let ns = ns.unwrap();
        let ns = match eenv.define_syntax(ns) {
            Ok(ns) => {
                println!("raw: {}:{}: {}", ns.source_location.0, ns.source_location.1, ns);
                ns
            },
            Err(e) => {
                println!("error: {}", e);
                continue;
            }
        };

        match eenv.expand_syntax(ns) {
            Ok(e) => {
                println!("resolved: {}", e);
                e
            },
            Err(e) => {
                println!("error: {}", e);
                continue;
            }
        };
    }
}
