mod io;

fn main() {
    let mut stdin = std::io::stdin().lock();
    let mut handle = io::InputPort::try_from(&mut stdin as &mut dyn std::io::Read).unwrap();
    loop {
        let source_location = handle.location();
        match io::Parser::next(&mut handle) {
            Ok(d) => println!("{}:{}: {:#?}", source_location.0, source_location.1, d),
            Err(e) => {
                eprintln!("Error: {}:{}: {:#?}", source_location.0, source_location.1, e);
                break;
            }
        }
    }
}
