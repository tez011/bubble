mod anf;
mod common;
mod cps;
mod io;
mod syntax;

fn main() {
    let mut env = common::Environment::new();
    let mut stdin = std::io::stdin().lock();
    let mut handle = io::InputPort::try_from(&mut stdin as &mut dyn std::io::Read).unwrap();
    loop {
        let se = match syntax::read(&mut handle) {
            Ok(stx) => match syntax::expand(stx, &mut env) {
                Ok(se) => {
                    eprintln!("resolved: {:#?}", se);
                    se
                },
                Err(e) => {
                    eprintln!("expand error at {:?}: {}", handle.location(), e);
                    continue;
                },
            },
            Err(e) => {
                eprintln!("parse error at {:?}: {}", handle.location(), e);
                continue;
            },
        };
        if let syntax::Expression::ProcedureCall { operator, .. } = &se {
            if let syntax::Expression::CorePrimitive(operator) = operator.as_ref() {
                if *operator == "__debug_dump_macros" {
                    env.bound_names().filter_map(|name| env.macro_rules(name).map(|r| (name, r)))
                        .for_each(|(name, rules)| println!("(define-syntax {} {})", name, rules));
                    continue;
                }
            }
        }

        let se = cps::transform(se, cps::ContinuationRef::Escape, &mut env).unwrap();
        println!("cps: {:#?}", se);

        let se = anf::transform(se, &env);
        println!("final form: {:#?}", se);
    }
}
