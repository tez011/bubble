
fn main() {
    let mut stdin = std::io::stdin().lock();
    let mut handle = bubble::io::InputPort::try_from(&mut stdin as &mut dyn std::io::Read).unwrap();
    let mut env = bubble::compiler::FrontendEnvironment::new();
    loop {
        let stx = bubble::compiler::read_syntax(&mut handle).unwrap();
        println!("raw: {}", stx);
        let stx = bubble::compiler::expand_syntax(stx, &mut env).unwrap();
        println!("expanded: {}", stx);
        let e = bubble::compiler::normalize_syntax(stx, &mut env).unwrap();
        println!("final:");
        for lambda in e {
            println!("{}", lambda);
        }
    }
}
