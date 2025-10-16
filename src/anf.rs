use crate::common::Arity;
use crate::cps;
use crate::cps::{Atom, Continuation, ContinuationDef, ContinuationRef, ValueID};
use crate::syntax;
use std::collections::HashSet;
use std::ops::ControlFlow;
const BETA_EXPANSION_COMPLEXITY_THRESHOLD: usize = 11;

#[derive(Debug)]
struct Environment<'stx> {
    stx_env: &'stx syntax::Environment,
    visible_bindings: HashSet<ValueID>,
    current_return: cps::ContinuationRef,
}
impl<'stx> From<&'stx syntax::Environment> for Environment<'stx> {
    fn from(stx_env: &'stx syntax::Environment) -> Self {
        Self {
            stx_env,
            visible_bindings: stx_env.bound_variables().map(ValueID).collect(),
            current_return: ContinuationRef::Escape,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Lambda {
    pub(crate) formals: (Vec<ValueID>, Option<ValueID>),
    pub(crate) body: Box<Expression>,
}

#[derive(Debug, Clone)]
pub enum Rhs {
    Literal(syntax::LiteralD),
    Lambda(Lambda),
    Values { values: (Vec<Atom>, Option<ValueID>) },
    Apply { operator: Atom, operands: (Vec<Atom>, Option<ValueID>) },
}

#[derive(Debug, Clone, Default)]
pub enum Expression {
    #[default] Nop,
    Let { id: (Vec<ValueID>, Option<ValueID>), val: Rhs, body: Box<Expression> },
    Branch { test: Atom, consequent: Box<Expression>, alternate: Box<Expression> },
    Return { values: (Vec<Atom>, Option<ValueID>) },
    TailCall { to: ValueID, values: (Vec<Atom>, Option<ValueID>) },
    Halt(bool),
}

impl Expression {
    fn visit<T>(&self, init: T, mut f: impl FnMut(T, &Expression) -> ControlFlow<T, T>) -> ControlFlow<T, T> {
        fn visit_children<T>(expr: &Expression, init: T, f: &mut impl FnMut(T, &Expression) -> ControlFlow<T, T>) -> ControlFlow<T, T> {
            use Expression::*;
            match expr {
                Let { val: Rhs::Lambda(val), body, .. } => {
                    let r = visit_children(val.body.as_ref(), init, f)?;
                    let r = f(r, val.body.as_ref())?;
                    let r = visit_children(body.as_ref(), r, f)?;
                    f(r, body.as_ref())
                },
                Let { body, .. } => {
                    let r = visit_children(body.as_ref(), init, f)?;
                    f(r, body.as_ref())
                },
                Branch { consequent, alternate, .. } => {
                    let r = visit_children(consequent.as_ref(), init, f)?;
                    let r = f(r, consequent.as_ref())?;
                    let r = visit_children(alternate.as_ref(), r, f)?;
                    f(r, alternate.as_ref())
                },
                Return { .. } | TailCall { .. } | Nop | Halt(_) => ControlFlow::Continue(init),
            }
        }

        let r = visit_children(self, init, &mut f)?;
        f(r, self)
    }
    fn visit_mut<T>(&mut self, init: T, mut f: impl FnMut(T, &mut Expression) -> ControlFlow<T, T>) -> ControlFlow<T, T> {
        fn visit_children<T>(expr: &mut Expression, init: T, f: &mut impl FnMut(T, &mut Expression) -> ControlFlow<T, T>) -> ControlFlow<T, T> {
            use Expression::*;
            match expr {
                Let { val: Rhs::Lambda(val), body, .. } => {
                    let r = visit_children(val.body.as_mut(), init, f)?;
                    let r = f(r, val.body.as_mut())?;
                    let r = visit_children(body.as_mut(), r, f)?;
                    f(r, body.as_mut())
                },
                Let { body, .. } => {
                    let r = visit_children(body.as_mut(), init, f)?;
                    f(r, body.as_mut())
                },
                Branch { consequent, alternate, .. } => {
                    let r = visit_children(consequent.as_mut(), init, f)?;
                    let r = f(r, consequent.as_mut())?;
                    let r = visit_children(alternate.as_mut(), r, f)?;
                    f(r, alternate.as_mut())
                },
                Return { .. } | TailCall { .. } | Nop | Halt(_) => ControlFlow::Continue(init),
            }
        }

        let r = visit_children(self, init, &mut f)?;
        f(r, self)
    }

    fn complexity(&self) -> usize {
        self.visit(0, |count, _| ControlFlow::Continue(count + 1))
            .continue_value().unwrap()
    }
    fn count_uses(&self, target: ValueID) -> usize {
        self.visit(0, |count, e| {
            use Expression::*;
            ControlFlow::Continue(count + match e {
                Branch { test, .. } => match test {
                    Atom::Variable(test) => if target == *test { 1 } else { 0 },
                    _ => 0,
                },
                Let { id, val, .. } => {
                    let lhs_uses = id.0.iter()
                        .chain(id.1.iter())
                        .filter(|&&i| i == target).count();
                    let rhs_uses = match val {
                        Rhs::Values { values } => values.0.iter()
                            .filter_map(|a| if let Atom::Variable(a) = a { Some(*a) } else { None })
                            .chain(values.1.iter().copied())
                            .filter(|&i| i == target).count(),
                        Rhs::Apply { operator, operands } => std::iter::once(operator)
                            .chain(operands.0.iter())
                            .filter_map(|a| if let Atom::Variable(a) = a { Some(*a) } else { None })
                            .chain(operands.1.iter().copied())
                            .filter(|&i| i == target).count(),
                        _ => 0,
                    };
                    lhs_uses + rhs_uses
                },
                Return { values } | TailCall { values, .. } => {
                    values.0.iter()
                        .filter_map(|a| if let Atom::Variable(a) = a { Some(*a) } else { None })
                        .chain(values.1.iter().copied())
                        .filter(|&i| i == target).count()
                },
                Nop | Halt(_) => 0,
            })
        }).continue_value().unwrap()
    }
    fn rename(&mut self, from: &(Vec<ValueID>, Option<ValueID>), to: &(Vec<Atom>, Option<ValueID>)) -> bool {
        assert!(from.0.len() <= to.0.len());
        let mapping = std::iter::zip(from.0.iter().copied(), to.0.iter().copied()).collect::<std::collections::HashMap<_, _>>();
        let tail_mapping = (&to.0[from.0.len() ..], to.1);
        let map_id = |i: &mut ValueID| {
            if let Some(Atom::Variable(mapped)) = mapping.get(i).copied() {
                *i = mapped;
                true
            } else {
                false
            }
        };
        let map_atom = |a: &mut Atom| {
            if let Atom::Variable(i) = a {
                if let Some(&mapped) = mapping.get(i) {
                    *a = mapped;
                    return true;
                }
            };
            false
        };
        let map_atoms = |aa: &mut (Vec<Atom>, Option<ValueID>)| {
            let size_hint = aa.0.len();
            let (mut modified, mut new_0) = aa.0.iter_mut().fold((false, Vec::with_capacity(size_hint)), |(unmodified, mut dest), a| {
                if let Atom::Variable(i) = a {
                    if let Some(&mapped) = mapping.get(i) {
                        dest.push(mapped);
                        return (true, dest);
                    } else if from.1.is_some_and(|tail| *i == tail) {
                        dest.extend(tail_mapping.0);
                        dest.extend(tail_mapping.1.iter().copied().map(Atom::Variable)); // the runtime may treat this as a list.
                        return (true, dest);
                    }
                }
                dest.push(*a);
                (unmodified, dest)
            });
            let new_1 = if aa.1 == from.1 {
                modified = true;
                new_0.extend(tail_mapping.0);
                tail_mapping.1
            } else {
                aa.1
            };
            if modified {
                *aa = (new_0, new_1);
            }
            modified
        };

        self.visit_mut(false, |unchanged, e| {
            use Expression::*;
            ControlFlow::Continue(match e {
                Let { val: Rhs::Values { values }, .. } => map_atoms(values),
                Let { val: Rhs::Apply { operator, operands }, .. } => map_atom(operator) | map_atoms(operands),
                Branch { test, .. } => map_atom(test),
                Return { values } => map_atoms(values),
                TailCall { to, values } => map_id(to) | map_atoms(values),
                _ => unchanged,
            })
        }).continue_value().unwrap()
    }
}

impl<'stx> Environment<'stx> {
    fn new_variables(&self, arity: Arity) -> (Vec<ValueID>, Option<ValueID>) {
        let arity = match arity {
            Arity::Exact(n) => (n, None),
            Arity::AtLeast(n) => (n, Some(ValueID(self.stx_env.new_variable()))),
            Arity::Unknown => (0, Some(ValueID(self.stx_env.new_variable()))),
            // TODO this is a temporary fix. We should store/load arity of visible bindings.
        };
        ((0..arity.0).map(|_| ValueID(self.stx_env.new_variable())).collect(), arity.1)
    }
    fn with_return<R>(&mut self, k: cps::ContinuationRef, f: impl FnOnce(&mut Self) -> R) -> R {
        let old = std::mem::replace(&mut self.current_return, k);
        let r = f(self);
        self.current_return = old;
        r
    }
    fn is_escape(&self, k: &cps::Continuation) -> bool {
        match k {
            cps::Continuation::Ref(cps::ContinuationRef::Escape) => true,
            cps::Continuation::Ref(k) if *k == self.current_return => true,
            _ => false,
        }
    }

    fn transform_cps(&mut self, e: cps::Expression) -> Expression {
        use Expression::*;
        match e {
            cps::Expression::LetLiteral { id, val, body } => Let {
                id: (vec![id.into()], None),
                val: Rhs::Literal(val),
                body: Box::new(self.transform_cps(*body)),
            },
            cps::Expression::LetLambda { id, val, body } => self.with_return(val.escape, |env| Let {
                id: (vec![id.into()], None),
                val: Rhs::Lambda(Lambda {
                    formals: val.formals,
                    body: Box::new(env.transform_cps(*val.body)),
                }),
                body: Box::new(env.transform_cps(*body)),
            }),
            cps::Expression::LetContinuation { id, k, body } => Let {
                id: (vec![id.into()], None),
                val: Rhs::Lambda(Lambda {
                    formals: k.formals,
                    body: Box::new(self.transform_cps(*k.body)),
                }),
                body: Box::new(self.transform_cps(*body)),
            },
            cps::Expression::Apply { operator, operands, k, kontract } => {
                if self.is_escape(&k) {
                    let id = self.new_variables(kontract);
                    let values = (id.0.iter().copied().map(Atom::Variable).collect(), id.1);
                    Let {
                        id,
                        val: Rhs::Apply { operator, operands },
                        body: Box::new(Return { values }),
                    }
                } else if let Continuation::Def(ContinuationDef { formals, body }) = k {
                    Let {
                        id: formals,
                        val: Rhs::Apply { operator, operands },
                        body: Box::new(self.transform_cps(*body)),
                    }
                } else if let Continuation::Ref(ContinuationRef::To(k)) = k {
                    let id = self.new_variables(kontract);
                    let values = (id.0.iter().copied().map(Atom::Variable).collect(), id.1);
                    Let {
                        id,
                        val: Rhs::Apply { operator, operands },
                        body: Box::new(TailCall { to: k.into(), values }),
                    }
                } else if let Continuation::Ref(k) = k {
                    if k == ContinuationRef::Abort {
                        Halt(false)
                    } else {
                        unreachable!()
                    }
                } else {
                    unreachable!()
                }
            }
            cps::Expression::Branch { test, consequent, alternate } => Branch {
                test,
                consequent: Box::new(self.transform_cps(*consequent)),
                alternate: Box::new(self.transform_cps(*alternate)),
            },
            cps::Expression::Continue { k, values } => {
                if self.is_escape(&k) {
                    Return { values }
                } else if let Continuation::Def(ContinuationDef { formals, body }) = k {
                    let mut body = self.transform_cps(*body);
                    body.rename(&formals, &values);
                    body
                } else if let Continuation::Ref(ContinuationRef::To(k)) = k {
                    TailCall { to: k.into(), values }
                } else if let Continuation::Ref(k) = k {
                    if k == ContinuationRef::Abort {
                        Halt(false)
                    } else {
                        unreachable!()
                    }
                } else {
                    unreachable!()
                }
            }
            cps::Expression::Assign { vars, values, k } => Let {
                id: vars,
                val: Rhs::Values { values },
                body: Box::new(if self.is_escape(&k) {
                    Return { values: (vec![], None) }
                } else if let Continuation::Def(ContinuationDef { formals: _, body }) = k {
                    // rename is unnecessary since formals should be empty
                    self.transform_cps(*body)
                } else if let Continuation::Ref(ContinuationRef::To(k)) = k {
                    TailCall { to: k.into(), values: (vec![], None) }
                } else if let Continuation::Ref(k) = k {
                    if k == ContinuationRef::Abort {
                        Halt(false)
                    } else {
                        unreachable!()
                    }
                } else {
                    unreachable!()
                })
            },
            cps::Expression::TakenOut => unreachable!(),
        }
    }

    fn apply_primitive_to_literals(&mut self, operator: &'static str, operands: &Vec<Atom>) -> Option<Rhs> {
        match operator {
            "+" if operands.iter().all(|a| matches!(a, Atom::Literal(syntax::LiteralC::Integer(_)))) => {
                let sum = operands.iter().map(|a| match a {
                    Atom::Literal(syntax::LiteralC::Integer(i)) => *i,
                    _ => unreachable!(),
                }).reduce(|acc, x| acc + x).unwrap_or(0);
                Some(Rhs::Values { values: (vec![Atom::Literal(syntax::LiteralC::Integer(sum))], None) })
            },
            _ => None,
        }
    }

    fn optimize(&mut self, e: &mut Expression) -> bool {
        use Expression::*;
        eprintln!("anf::optimize: {:?}", e);
        match e {
            Let { id, val: Rhs::Literal(_), body } => {
                if id.0.len() != 1 || id.1.is_some() { unreachable!(); }
                let id = id.0.first_mut().unwrap();
                if !self.visible_bindings.contains(id) && body.count_uses(*id) == 0 {
                    *e = std::mem::take(body);
                    true
                } else {
                    false
                }
            },
            Let { id, val: Rhs::Lambda(lambda), body } => {
                if id.0.len() != 1 || id.1.is_some() { unreachable!(); }
                let lambda_id = id.0.first_mut().unwrap();
                let uses = body.count_uses(*lambda_id);
                let applies = body.visit(0, |count: usize, e: &Expression| {
                    ControlFlow::Continue(count + match e {
                        Let { val: Rhs::Apply { operator: Atom::Variable(operator), .. }, .. } => if lambda_id == operator { 1 } else { 0 },
                        TailCall { to, .. } => if lambda_id == to { 1 } else { 0 },
                        _ => 0,
                    })
                }).continue_value().unwrap();
                let is_recursive = match lambda.body.visit(false, |is_recursive, e| match e {
                    Let { val: Rhs::Apply { operator: Atom::Variable(operator), .. }, .. } if lambda_id == operator => ControlFlow::Break(true),
                    TailCall { to, .. } if lambda_id == to => ControlFlow::Break(true),
                    _ => ControlFlow::Continue(is_recursive),
                }) {
                    ControlFlow::Continue(x) => x,
                    ControlFlow::Break(x) => x,
                };
                if !self.visible_bindings.contains(lambda_id) && uses == 0 {
                    *e = std::mem::take(body);
                    true
                } else if uses == applies && !is_recursive && (applies == 1 || lambda.body.complexity() < BETA_EXPANSION_COMPLEXITY_THRESHOLD) {
                    body.visit_mut((), |_, e| match e {
                        Let { id, val: Rhs::Apply { operator: Atom::Variable(operator), operands }, body } if lambda_id == operator => {
                            let mut n = *if applies == 1 { std::mem::take(&mut lambda.body) } else { lambda.body.clone() };
                            n.rename(&lambda.formals, &operands);
                            n.visit_mut((), |_, e| {
                                if let Return { values } = e {
                                    *e = Let { id: id.clone(), val: Rhs::Values { values: values.clone() }, body: body.clone() };
                                }
                                ControlFlow::Continue(())
                            });
                            *e = n;
                            ControlFlow::Continue(())
                        },
                        TailCall { to, values } if lambda_id == to => {
                            let mut n = *if applies == 1 { std::mem::take(&mut lambda.body) } else { lambda.body.clone() };
                            n.rename(&lambda.formals, &values);
                            *e = n;
                            ControlFlow::Continue(())
                        },
                        _ => ControlFlow::Continue(()),
                    });
                    *e = std::mem::take(body);
                    true
                } else {
                    false
                }
            },
            Let { id, val: Rhs::Values { values }, body } => {
                if id.0.iter().chain(id.1.iter()).all(|i| !self.visible_bindings.contains(i)) {
                    body.rename(&id, &values);
                    *e = std::mem::take(body);
                    true
                } else {
                    false
                }
            },
            Let { id, val: Rhs::Apply { operator: Atom::CorePrimitive(operator), operands: (operands, None) }, body } => {
                if let Some(val) = self.apply_primitive_to_literals(operator, operands) {
                    *e = Let {
                        id: std::mem::take(id),
                        val,
                        body: std::mem::take(body),
                    };
                    true
                } else {
                    false
                }
            },
            _ => false,
        }
    }
}

pub fn transform(e: cps::Expression, env: &syntax::Environment) -> Expression {
    let mut env = Environment::from(env);
    let mut e = env.transform_cps(e);

    loop {
        if e.visit_mut((), |_, e| {
            if env.optimize(e) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }).is_continue() {
            return e;
        } else {
            eprintln!("*** optimize starting over");
        }
    }
}
