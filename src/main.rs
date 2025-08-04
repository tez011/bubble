mod io;
mod parser;

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
    List(Vec<Datum>, bool),
    Vector(Vec<Datum>),
}

fn main() {
    let mut stdin = std::io::stdin().lock();
    let mut handle = io::InputPort::try_from(&mut stdin as &mut dyn std::io::Read).unwrap();
    loop {
        let ns = parser::next(&mut handle);
        if ns.is_err() {
            let location = handle.location();
            eprintln!("Parse error: {}:{}: {}", location.0, location.1, ns.unwrap_err());
            break;
        }

        let ns = ns.unwrap();
        let location = ns.source_location;
        println!("{}:{}: {:?}", location.0, location.1, Datum::from(ns.clone()));

        // let expanded = ns.expand();
        // if expanded.is_err() {
        //     eprintln!("Expansion error: {}:{}: {:?}", location.0, location.1, expanded.unwrap_err());
        //     break;
        // }
        // println!("{:?}", expanded.unwrap());
    }
}
