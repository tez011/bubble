mod io;
mod syntax;
use std::cell::Cell;

#[derive(Debug)]
pub struct Environment {
    macros: std::collections::VecDeque<syntax::Rules>,
    bindings: std::collections::HashMap<std::borrow::Cow<'static, str>, std::collections::BTreeMap<syntax::ScopeSet, syntax::Binding>>,
    scope_counter: Cell<usize>,
    bound_id_counter: Cell<usize>,
}

fn main() {
    let mut e_env = Environment::new();
    for (name, bindings) in e_env.bindings.iter() {
        match bindings.get(&Default::default()) {
            Some(syntax::Binding::SyntaxTransformer(i)) => {
                println!("(define-syntax {} {})", name, e_env.macros[*i]);
            },
            _ => (),
        }
    }

    let mut stdin = std::io::stdin().lock();
    let mut handle = io::InputPort::try_from(&mut stdin as &mut dyn std::io::Read).unwrap();

    loop {
        let stx = match syntax::read(&mut handle) {
            Ok(stx) => stx,
            Err(e) => { eprintln!("Parse error at {:?}: {}", handle.location(), e); continue; },
        };
        let se = match syntax::expand(stx, &mut e_env) {
            Ok(se) => se,
            Err(e) => {
                eprintln!("Expand error at {:?}: {}", handle.location(), e);
                continue;
            }
        };
        println!("final form: {:#?}", se);
    }
}
