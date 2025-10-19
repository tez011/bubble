use crate::common::{Arity, Environment as CommonEnvironment};
use crate::cps;
use crate::cps::{Atom, Continuation, ContinuationDef, ContinuationRef, ValueID};
use crate::syntax;
use std::collections::HashSet;
use std::ops::ControlFlow;
const BETA_EXPANSION_COMPLEXITY_THRESHOLD: usize = 11;

#[derive(Debug)]
struct Environment<'c> {
    parent_env: &'c CommonEnvironment,
    current_return: cps::ContinuationRef,
}
impl<'c> From<&'c CommonEnvironment> for Environment<'c> {
    fn from(parent_env: &'c CommonEnvironment) -> Self {
        Self {
            parent_env,
            current_return: ContinuationRef::Escape,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Lambda {
    pub(crate) formals: (Vec<ValueID>, Option<ValueID>),
    pub(crate) closed_env: Vec<ValueID>,
    pub(crate) body: Box<Expression>,
}

#[derive(Debug, Clone)]
pub enum Rhs {
    Literal(syntax::LiteralD),
    Lambda(Lambda),
    Closure { index: usize, closed_env: Vec<Atom> },
}
#[derive(Debug, Clone)]
pub enum Rhses {
    Values { values: (Vec<Atom>, Option<ValueID>) },
    Apply { operator: Atom, operands: (Vec<Atom>, Option<ValueID>) },
}

#[derive(Debug, Clone, Default)]
pub enum Expression {
    #[default] Nop,
    Let { id: ValueID, val: Rhs, body: Box<Expression> },
    LetValues { ids: (Vec<ValueID>, Option<ValueID>), val: Rhses, body: Box<Expression> },
    Branch { test: Atom, consequent: Box<Expression>, alternate: Box<Expression> },
    Return { values: (Vec<Atom>, Option<ValueID>) },
    TailCall { to: ValueID, values: (Vec<Atom>, Option<ValueID>) },
    Halt,
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
                Let { body, .. } | LetValues { body, .. } => {
                    let r = visit_children(body.as_ref(), init, f)?;
                    f(r, body.as_ref())
                },
                Branch { consequent, alternate, .. } => {
                    let r = visit_children(consequent.as_ref(), init, f)?;
                    let r = f(r, consequent.as_ref())?;
                    let r = visit_children(alternate.as_ref(), r, f)?;
                    f(r, alternate.as_ref())
                },
                Return { .. } | TailCall { .. } | Nop | Halt => ControlFlow::Continue(init),
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
                Let { body, .. } | LetValues { body, .. } => {
                    let r = visit_children(body.as_mut(), init, f)?;
                    f(r, body.as_mut())
                },
                Branch { consequent, alternate, .. } => {
                    let r = visit_children(consequent.as_mut(), init, f)?;
                    let r = f(r, consequent.as_mut())?;
                    let r = visit_children(alternate.as_mut(), r, f)?;
                    f(r, alternate.as_mut())
                },
                Return { .. } | TailCall { .. } | Nop | Halt => ControlFlow::Continue(init),
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
                Let { .. } => 0,
                LetValues { val, .. } => match val {
                    Rhses::Values { values } => values.0.iter()
                        .filter_map(|a| if let Atom::Variable(a) = a { Some(*a) } else { None })
                        .chain(values.1.iter().copied())
                        .filter(|&i| i == target).count(),
                    Rhses::Apply { operator, operands } => std::iter::once(operator)
                        .chain(operands.0.iter())
                        .filter_map(|a| if let Atom::Variable(a) = a { Some(*a) } else { None })
                        .chain(operands.1.iter().copied())
                        .filter(|&i| i == target).count(),
                },
                Branch { test, .. } => match test {
                    Atom::Variable(test) => if target == *test { 1 } else { 0 },
                    _ => 0,
                },
                Return { values } | TailCall { values, .. } => {
                    values.0.iter()
                        .filter_map(|a| if let Atom::Variable(a) = a { Some(*a) } else { None })
                        .chain(values.1.iter().copied())
                        .filter(|&i| i == target).count()
                },
                Nop | Halt => 0,
            })
        }).continue_value().unwrap()
    }

    /// Returns a tuple of (variables introduced, variables used).
    fn variables(&self) -> (HashSet<ValueID>, HashSet<ValueID>) {
        use Expression::*;
        self.visit((HashSet::new(), HashSet::new()), |(mut intros, mut uses), e| {
            match e {
                Let { id, val, .. } => {
                    intros.insert(*id);
                    match val {
                        Rhs::Literal(_) => (),
                        Rhs::Lambda(Lambda { formals, closed_env, .. }) => {
                            intros.extend(formals.0.iter().chain(formals.1.iter()).copied());
                            uses.extend(closed_env.iter().copied());
                        },
                        Rhs::Closure { closed_env, .. } => {
                            uses.extend(closed_env.iter()
                                .filter_map(|a| if let Atom::Variable(i) = a { Some(*i) } else { None }));
                        },
                    };
                }
                LetValues { ids, val, .. } => {
                    intros.extend(ids.0.iter().chain(ids.1.iter()).copied());
                    match val {
                        Rhses::Values { values } => {
                            uses.extend(values.0.iter()
                                .filter_map(|a| if let Atom::Variable(i) = a { Some(*i) } else { None })
                                .chain(values.1.iter().copied()));
                        },
                        Rhses::Apply { operator, operands } => {
                            uses.extend(operands.0.iter()
                                .filter_map(|a| if let Atom::Variable(i) = a { Some(*i) } else { None })
                                .chain(operands.1.iter().copied()));
                            if let Atom::Variable(i) = operator {
                                uses.insert(*i);
                            }
                        },
                    }
                },
                Branch { test, .. } => {
                    if let Atom::Variable(i) = test {
                        uses.insert(*i);
                    }
                },
                Return { values } => {
                    uses.extend(values.0.iter()
                        .filter_map(|a| if let Atom::Variable(i) = a { Some(*i) } else { None })
                        .chain(values.1.iter().copied()));
                }
                TailCall { to, values } => {
                    uses.insert(*to);
                    uses.extend(values.0.iter()
                        .filter_map(|a| if let Atom::Variable(i) = a { Some(*i) } else { None })
                        .chain(values.1.iter().copied()));
                }
                Nop | Halt => (),
            }
            ControlFlow::Continue((intros, uses))
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
                Let { .. } => unchanged,
                LetValues { val: Rhses::Values { values }, .. } => map_atoms(values),
                LetValues { val: Rhses::Apply { operator, operands }, .. } => map_atom(operator) | map_atoms(operands),
                Branch { test, .. } => map_atom(test),
                Return { values } => map_atoms(values),
                TailCall { to, values } => map_id(to) | map_atoms(values),
                Nop | Halt => unchanged,
            })
        }).continue_value().unwrap()
    }
}

impl<'stx> Environment<'stx> {
    fn new_variables(&self, arity: Arity) -> (Vec<ValueID>, Option<ValueID>) {
        let arity = match arity {
            Arity::Exact(n) => (n, None),
            Arity::AtLeast(n) => (n, Some(ValueID(self.parent_env.new_variable()))),
            Arity::Unknown => (0, Some(ValueID(self.parent_env.new_variable()))),
        };
        ((0..arity.0).map(|_| ValueID(self.parent_env.new_variable())).collect(), arity.1)
    }
    fn binding_visible(&self, id: &ValueID) -> bool {
        self.parent_env.bound_variables.contains(&id.0)
    }
    fn is_escape(&self, k: &cps::Continuation) -> bool {
        match k {
            cps::Continuation::Ref(cps::ContinuationRef::Escape) => true,
            cps::Continuation::Ref(k) if *k == self.current_return => true,
            _ => false,
        }
    }
    fn with_return<R>(&mut self, k: cps::ContinuationRef, f: impl FnOnce(&mut Self) -> R) -> R {
        let old = std::mem::replace(&mut self.current_return, k);
        let r = f(self);
        self.current_return = old;
        r
    }

    fn transform(&mut self, e: cps::Expression) -> Expression {
        use Expression::*;
        match e {
            cps::Expression::LetLiteral { id, val, body } => Let {
                id,
                val: Rhs::Literal(val),
                body: Box::new(self.transform(*body)),
            },
            cps::Expression::LetLambda { id, val, body } => Let {
                id,
                val: Rhs::Lambda(Lambda {
                    formals: val.formals,
                    closed_env: vec![],
                    body: Box::new(self.with_return(val.escape, |env| env.transform(*val.body))),
                }),
                body: Box::new(self.transform(*body)),
            },
            cps::Expression::LetContinuation { id, k, body } => Let {
                id: id.into(),
                val: Rhs::Lambda(Lambda {
                    formals: k.formals,
                    closed_env: vec![],
                    body: Box::new(self.transform(*k.body)),
                }),
                body: Box::new(self.transform(*body)),
            },
            cps::Expression::Apply { operator, operands, k, kontract } => {
                if self.is_escape(&k) {
                    let ids = self.new_variables(kontract);
                    let values = (ids.0.iter().copied().map(Atom::Variable).collect(), ids.1);
                    LetValues {
                        ids,
                        val: Rhses::Apply { operator, operands },
                        body: Box::new(Return { values }),
                    }
                } else if let Continuation::Def(ContinuationDef { formals, body }) = k {
                    LetValues {
                        ids: formals,
                        val: Rhses::Apply { operator, operands },
                        body: Box::new(self.transform(*body)),
                    }
                } else if let Continuation::Ref(ContinuationRef::To(k)) = k {
                    let ids = self.new_variables(kontract);
                    let values = (ids.0.iter().copied().map(Atom::Variable).collect(), ids.1);
                    LetValues {
                        ids,
                        val: Rhses::Apply { operator, operands },
                        body: Box::new(TailCall { to: k.into(), values }),
                    }
                } else if let Continuation::Ref(k) = k {
                    if k == ContinuationRef::Abort {
                        Halt
                    } else {
                        unreachable!()
                    }
                } else {
                    unreachable!()
                }
            }
            cps::Expression::Branch { test, consequent, alternate } => Branch {
                test,
                consequent: Box::new(self.transform(*consequent)),
                alternate: Box::new(self.transform(*alternate)),
            },
            cps::Expression::Continue { k, values } => {
                if self.is_escape(&k) {
                    Return { values }
                } else if let Continuation::Def(ContinuationDef { formals, body }) = k {
                    let mut body = self.transform(*body);
                    body.rename(&formals, &values);
                    body
                } else if let Continuation::Ref(ContinuationRef::To(k)) = k {
                    TailCall { to: k.into(), values }
                } else if let Continuation::Ref(k) = k {
                    if k == ContinuationRef::Abort {
                        Halt
                    } else {
                        unreachable!()
                    }
                } else {
                    unreachable!()
                }
            }
            cps::Expression::Assign { vars, values, k } => LetValues {
                ids: vars,
                val: Rhses::Values { values },
                body: Box::new(if self.is_escape(&k) {
                    Return { values: (vec![], None) }
                } else if let Continuation::Def(ContinuationDef { formals: _, body }) = k {
                    // rename is unnecessary since formals should be empty
                    self.transform(*body)
                } else if let Continuation::Ref(ContinuationRef::To(k)) = k {
                    TailCall { to: k.into(), values: (vec![], None) }
                } else if let Continuation::Ref(k) = k {
                    if k == ContinuationRef::Abort {
                        Halt
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

    fn apply_primitive_to_literals(&mut self, operator: &'static str, operands: &Vec<Atom>) -> Option<Rhses> {
        match operator {
            "+" if operands.iter().all(|a| matches!(a, Atom::Literal(syntax::LiteralC::Integer(_)))) => {
                let sum = operands.iter().map(|a| match a {
                    Atom::Literal(syntax::LiteralC::Integer(i)) => *i,
                    _ => unreachable!(),
                }).reduce(|acc, x| acc + x).unwrap_or(0);
                Some(Rhses::Values { values: (vec![Atom::Literal(syntax::LiteralC::Integer(sum))], None) })
            },
            _ => None,
        }
    }

    fn optimize(&mut self, e: &mut Expression) -> bool {
        use Expression::*;
        eprintln!("anf::optimize: {:?}", e);
        match e {
            Let { id, val: Rhs::Literal(_), body } => {
                if self.binding_visible(id) {
                    false
                } else if body.count_uses(*id) == 0 {
                    *e = std::mem::take(body);
                    true
                } else {
                    false
                }
            },
            Let { id: lambda_id, val: Rhs::Lambda(lambda), body } => {
                let uses = body.count_uses(*lambda_id);
                let applies = body.visit(0, |count: usize, e: &Expression| {
                    ControlFlow::Continue(count + match e {
                        LetValues { val: Rhses::Apply { operator: Atom::Variable(operator), .. }, .. } => if lambda_id == operator { 1 } else { 0 },
                        TailCall { to, .. } => if lambda_id == to { 1 } else { 0 },
                        _ => 0,
                    })
                }).continue_value().unwrap();
                let is_recursive = match lambda.body.visit(false, |is_recursive, e| match e {
                    LetValues { val: Rhses::Apply { operator: Atom::Variable(operator), .. }, .. } if lambda_id == operator => ControlFlow::Break(true),
                    TailCall { to, .. } if lambda_id == to => ControlFlow::Break(true),
                    _ => ControlFlow::Continue(is_recursive),
                }) {
                    ControlFlow::Continue(x) => x,
                    ControlFlow::Break(x) => x,
                };
                if self.binding_visible(lambda_id) {
                    false
                } else if uses == 0 {
                    *e = std::mem::take(body);
                    true
                } else if uses == applies && !is_recursive && (applies == 1 || lambda.body.complexity() < BETA_EXPANSION_COMPLEXITY_THRESHOLD) {
                    body.visit_mut((), |_, e| match e {
                        LetValues { ids, val: Rhses::Apply { operator: Atom::Variable(operator), operands }, body } if lambda_id == operator => {
                            let mut n = *if applies == 1 { std::mem::take(&mut lambda.body) } else { lambda.body.clone() };
                            n.rename(&lambda.formals, &operands);
                            n.visit_mut((), |_, e| {
                                if let Return { values } = e {
                                    *e = LetValues { ids: ids.clone(), val: Rhses::Values { values: values.clone() }, body: body.clone() };
                                }
                                ControlFlow::Continue(())
                            }).continue_value().unwrap();
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
                    }).continue_value().unwrap();
                    *e = std::mem::take(body);
                    true
                } else {
                    false
                }
            },
            LetValues { ids, val: Rhses::Values { values }, body } => {
                if ids.0.iter().chain(ids.1.iter()).all(|i| !self.binding_visible(i)) {
                    body.rename(&ids, &values);
                    *e = std::mem::take(body);
                    true
                } else {
                    false
                }
            },
            LetValues { ids, val: Rhses::Apply { operator: Atom::CorePrimitive(operator), operands: (operands, None) }, body } => {
                if let Some(val) = self.apply_primitive_to_literals(operator, operands) {
                    *e = LetValues {
                        ids: std::mem::take(ids),
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

pub fn transform(e: cps::Expression, env: &CommonEnvironment) -> Expression {
    let mut env = Environment::from(env);
    let mut e = env.transform(e);
    eprintln!("unoptimized anf: {:#?}", e);

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

pub fn convert_closures(mut e: Expression) -> Vec<Lambda> {
    let mut lambdas = e.visit_mut(Vec::new(), |mut lambdas, e| {
        use Expression::*;
        if let Let { id, val: Rhs::Lambda(lambda), body } = e {
            let (mut intros, uses) = lambda.body.variables();
            intros.extend(lambda.formals.0.iter().chain(lambda.formals.1.iter()).copied());

            let index = lambdas.len();
            let closed_env = uses.difference(&intros).copied().collect::<Vec<_>>();
            let n_e = Let {
                id: *id,
                val: Rhs::Closure { index, closed_env: closed_env.iter().copied().map(Atom::Variable).collect() },
                body: std::mem::take(body),
            };
            lambdas.push(Lambda {
                formals: std::mem::take(&mut lambda.formals),
                closed_env,
                body: std::mem::take(&mut lambda.body),
            });
            *e = n_e;
        }
        ControlFlow::Continue(lambdas)
    }).continue_value().unwrap();

    lambdas.push(Lambda {
        formals: (vec![], None),
        closed_env: {
            let (intros, uses) = e.variables();
            uses.difference(&intros).copied().collect::<Vec<_>>()
        },
        body: Box::new(e),
    });
    lambdas
}
