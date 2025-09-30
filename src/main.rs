mod cps;
mod io;
mod syntax;

#[derive(Debug)]
pub struct Environment {
    macros: std::collections::VecDeque<syntax::Rules>,
    bindings: std::collections::HashMap<std::borrow::Cow<'static, str>, std::collections::BTreeMap<syntax::ScopeSet, syntax::Binding>>,
    scope_counter: std::cell::Cell<usize>,
    bound_id_counter: std::cell::Cell<usize>,
}

fn main() {
    let mut stdin = std::io::stdin().lock();
    let mut handle = io::InputPort::try_from(&mut stdin as &mut dyn std::io::Read).unwrap();
    let mut env = Environment::new();
    loop {
        let se = match syntax::read(&mut handle) {
            Ok(stx) => match syntax::expand(stx, &mut env) {
                Ok(se) => {
                    eprintln!("resolved: {:?}", se);
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

        let se = cps::transform(se, cps::Continuation::Halt, &env);
        if let cps::Expression::Apply { operator, .. } = se {
            if operator == cps::Atom::CorePrimitive("__library_export") {
                for (name, bindings) in env.bindings.iter() {
                    match bindings.get(&Default::default()) {
                        Some(syntax::Binding::SyntaxTransformer(i)) => {
                            println!("(define-syntax {} {})", name, env.macros[*i]);
                        },
                        _ => (),
                    }
                }
                continue;
            }
        }
        println!("cps: {:?}", se);

        let code = se.hoist_lambdas(cps::Continuation::Halt);
        println!("final form: {:#?}", code);
    }
}
