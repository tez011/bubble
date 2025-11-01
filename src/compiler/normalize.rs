use crate::compiler::{Arity, Atom, Binding, ValueID};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::ops::ControlFlow;
use std::rc::Rc;
use super::{frontend, syntax};
use super::Located;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ContinuationID(usize);
impl From<ValueID> for ContinuationID {
    fn from(value: ValueID) -> Self {
        Self(value.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum ContinuationRef {
    #[default] Escape,
    Abort(std::num::NonZero<u8>),
    To(ContinuationID),
}
#[derive(Debug, Clone)]
struct ContinuationDef {
    formals: (Vec<ValueID>, Option<ValueID>),
    body: Box<cps::Expression>,
}
#[derive(Debug, Clone)]
enum Continuation {
    Ref(ContinuationRef),
    Def(ContinuationDef),
}
impl Default for Continuation {
    fn default() -> Self {
        Continuation::Ref(Default::default())
    }
}
impl From<ContinuationID> for Continuation {
    fn from(value: ContinuationID) -> Self {
        Continuation::Ref(ContinuationRef::To(value))
    }
}

mod cps {
    use crate::compiler::{LiteralC, LiteralD, Location};
    use super::*;
    const BETA_EXPANSION_COMPLEXITY_THRESHOLD: usize = 6;

    #[derive(Debug, Clone)]
    pub(super) struct Lambda {
        pub(super) formals: (Vec<ValueID>, Option<ValueID>),
        pub(super) escape: ContinuationRef,
        pub(super) body: Box<Expression>,
    }

    #[derive(Debug, Clone, Default)]
    pub(crate) enum Expression {
        LetLiteral { id: ValueID, val: Rc<LiteralD>, body: Box<Expression> },
        LetLambda { id: ValueID, val: Lambda, body: Box<Expression> },
        LetContinuation { id: ContinuationID, k: ContinuationDef, body: Box<Expression> },
        Apply { operator: Atom, operands: (Vec<Atom>, Option<ValueID>), location: Location, kontract: Arity, k: Continuation },
        Branch { test: Atom, consequent: Box<Expression>, alternate: Box<Expression> },
        Continue { k: Continuation, values: (Vec<Atom>, Option<ValueID>) },
        Assign { vars: (Vec<ValueID>, Option<ValueID>), values: (Vec<Atom>, Option<ValueID>), k: Continuation },
        #[default] TakenOut,
    }
    impl Expression {
        pub(super) fn visit<T>(&self, init: T, mut f: impl FnMut(T, &Expression) -> ControlFlow<T, T>) -> ControlFlow<T, T> {
            fn visit_children<T>(expr: &Expression, init: T, f: &mut impl FnMut(T, &Expression) -> ControlFlow<T, T>) -> ControlFlow<T, T> {
                use Expression::*;
                match expr {
                    LetLiteral { body, .. } => {
                        let r = visit_children(body.as_ref(), init, f)?;
                        f(r, body.as_ref())
                    },
                    LetLambda { val, body, .. } => {
                        let r = visit_children(val.body.as_ref(), init, f)?;
                        let r = f(r, val.body.as_ref())?;
                        let r = visit_children(body.as_ref(), r, f)?;
                        f(r, body.as_ref())
                    },
                    LetContinuation { k, body, .. } => {
                        let r = visit_children(k.body.as_ref(), init, f)?;
                        let r = f(r, k.body.as_ref())?;
                        let r = visit_children(body.as_ref(), r, f)?;
                        f(r, body.as_ref())
                    },
                    Branch { consequent, alternate, .. } => {
                        let r = visit_children(consequent.as_ref(), init, f)?;
                        let r = f(r, consequent.as_ref())?;
                        let r = visit_children(alternate.as_ref(), r, f)?;
                        f(r, alternate.as_ref())
                    },
                    Apply { k: Continuation::Def(k), .. } | Continue { k: Continuation::Def(k), .. } | Assign { k: Continuation::Def(k), .. } => {
                        let r = visit_children(k.body.as_ref(), init, f)?;
                        f(r, k.body.as_ref())
                    },
                    Apply { .. } | Continue { .. } | Assign { .. } | TakenOut => ControlFlow::Continue(init),
                }
            }

            let r = visit_children(self, init, &mut f)?;
            f(r, self)
        }
        pub(super) fn visit_mut<T>(&mut self, init: T, mut f: impl FnMut(T, &mut Expression) -> ControlFlow<T, T>) -> ControlFlow<T, T> {
            fn visit_children<T>(expr: &mut Expression, init: T, f: &mut impl FnMut(T, &mut Expression) -> ControlFlow<T, T>) -> ControlFlow<T, T> {
                use Expression::*;
                match expr {
                    LetLiteral { body, .. } => {
                        let r = visit_children(body.as_mut(), init, f)?;
                        f(r, body.as_mut())
                    },
                    LetLambda { val, body, .. } => {
                        let r = visit_children(val.body.as_mut(), init, f)?;
                        let r = f(r, val.body.as_mut())?;
                        let r = visit_children(body.as_mut(), r, f)?;
                        f(r, body.as_mut())
                    },
                    LetContinuation { k, body, .. } => {
                        let r = visit_children(&mut k.body, init, f)?;
                        let r = f(r, &mut k.body)?;
                        let r = visit_children(body.as_mut(), r, f)?;
                        f(r, body.as_mut())
                    },
                    Branch { consequent, alternate, .. } => {
                        let r = visit_children(consequent.as_mut(), init, f)?;
                        let r = f(r, consequent.as_mut())?;
                        let r = visit_children(alternate.as_mut(), r, f)?;
                        f(r, alternate.as_mut())
                    },
                    Apply { k: Continuation::Def(k), .. } | Continue { k: Continuation::Def(k), .. } | Assign { k: Continuation::Def(k), .. } => {
                        let r = visit_children(k.body.as_mut(), init, f)?;
                        f(r, k.body.as_mut())
                    },
                    Apply { .. } | Continue { .. } | Assign { .. } | TakenOut => ControlFlow::Continue(init),
                }
            }

            let r = visit_children(self, init, &mut f)?;
            f(r, self)
        }

        pub(super) fn complexity(&self) -> usize {
            self.visit(0, |count, _| ControlFlow::Continue(count + 1))
                .continue_value().unwrap()
        }
        pub(super) fn uses_variable(&self, id: ValueID) -> usize {
            self.visit(0, |count, e| {
                use Expression::*;
                ControlFlow::Continue(count + match e {
                    Apply { operator, operands, .. } => std::iter::once(operator).chain(operands.0.iter()).filter(|&&a| a == Atom::Variable(id)).count() + (if operands.1.is_some_and(|i| i == id) { 1 } else { 0 }),
                    Branch { test, .. } => if *test == Atom::Variable(id) { 1 } else { 0 },
                    Continue { values, .. } | Assign { values, .. } => values.0.iter().filter(|&&a| a == Atom::Variable(id)).count() + (if values.1.is_some_and(|i| i == id) { 1 } else { 0 }),
                    _ => 0,
                })
            }).continue_value().unwrap()
        }

        pub(super) fn rename(&mut self, from: &(Vec<ValueID>, Option<ValueID>), to: &(Vec<Atom>, Option<ValueID>)) -> bool {
            assert!(from.0.len() <= to.0.len());
            let mapping = std::iter::zip(from.0.iter().copied(), to.0.iter().copied()).collect::<HashMap<_, _>>();
            let tail_mapping = (&to.0[from.0.len() ..], to.1);
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
                    Apply { operator, operands, .. } => map_atom(operator) | map_atoms(operands),
                    Branch { test, .. } => map_atom(test),
                    Continue { values, .. } | Assign { values, .. } => map_atoms(values),
                    _ => unchanged,
                })
            }).continue_value().unwrap()
        }

        pub(super) fn recontinue(&mut self, from: ContinuationRef, to: Continuation) -> bool {
            use Expression::*;
            let map_k = |k: &mut Continuation| match k {
                Continuation::Ref(kr) if *kr == from => {
                    *k = to.clone();
                    true
                },
                Continuation::Ref(_) => false,
                Continuation::Def(_) => false,
            };
            self.visit_mut(false, |modified, e| match e {
                Apply { k, .. } | Continue { k, .. } | Assign { k, .. } => ControlFlow::Continue(map_k(k) || modified),
                _ => ControlFlow::Continue(modified),
            }).continue_value().unwrap()
        }

        pub(super) fn hoist_assignment(&mut self, id: &mut ValueID) -> bool {
            use Expression::*;
            let Assign { vars, values, .. } = self else { panic!() };
            if let Some(index) = values.0.iter().enumerate().find_map(|(i, &v)| if v == Atom::Variable(*id) { Some(i) } else { None }) {
                values.0.swap_remove(index);
                *id = vars.0.swap_remove(index);
                true
            } else {
                false
            }
        }

        pub(super) fn check_arity(&mut self, env: &Environment) -> Result<HashMap<ValueID, (Arity, Arity)>, String> {
            use Expression::*;
            let mut io_lambdas = HashMap::new(); // The arity of inputs to/outputs from each lambda
            let mut mid_lambdas = HashMap::new();
            let mut continues = HashMap::new(); // The arity of inputs to each non-inline continuation
            let atom_name = |a: &Atom| match a {
                Atom::Variable(ValueID(x)) => match env.toplevels.iter().find(|(_, binding)| **binding == Binding::Variable(*x)) {
                    Some((name, _)) => name.to_string(),
                    None => a.to_string(),
                }
                _ => a.to_string(),
            };

            self.visit((), |_, e| {
                if let LetLambda { id, val: cps::Lambda { formals, escape, body }, .. } = e {
                    io_lambdas.insert(*id, (Arity::from(formals), Arity::Unknown));
                    mid_lambdas.insert(*id, body.visit((None, HashSet::new()), |(explicits, mut applies), e| {
                        if let Continue { k: Continuation::Ref(k), values } = e {
                            if k == escape {
                                return match explicits {
                                    None => ControlFlow::Continue((Some(Arity::from(values)), applies)),
                                    Some(explicits) => ControlFlow::Continue((Some(explicits.join(Arity::from(values))), applies)),
                                };
                            }
                        } else if let Apply { operator, k: Continuation::Ref(k), .. } = e {
                            if k == escape {
                                if let Atom::Core(operator) = operator {
                                    return match explicits {
                                        None => ControlFlow::Continue((Some(operator.returns()), applies)),
                                        Some(explicits) => ControlFlow::Continue((Some(explicits.join(operator.returns())), applies)),
                                    };
                                } else if let Atom::Variable(x) = operator {
                                    if x != id { // if it's recursive, it can only return an arity we've already seen; ignore it.
                                        applies.insert(*x);
                                    }
                                }
                            }
                        }
                        ControlFlow::Continue((explicits, applies))
                    }).continue_value().unwrap());
                } else if let LetContinuation { id, k: ContinuationDef { formals, .. }, .. } = e {
                    continues.insert(*id, Arity::from(formals));
                }
                ControlFlow::Continue(())
            }).continue_value().unwrap();

            let mut mid_lambdas = mid_lambdas.into_iter().collect::<Vec<_>>();
            mid_lambdas.sort_by_key(|(_, (_, deps))| deps.len());
            for (id, (base_arity, deps)) in mid_lambdas.into_iter() {
                let dep_arity = deps.iter()
                    .map(|x| io_lambdas.get(x).map(|x| x.1).unwrap_or(Arity::Unknown))
                    .reduce(|acc, e| acc.join(e));
                io_lambdas.entry(id)
                    .and_modify(|arity| arity.1 = match (base_arity, dep_arity) {
                        (None, None) => Arity::Unknown,
                        (None, Some(x)) | (Some(x), None) => x,
                        (Some(a), Some(b)) => a.join(b),
                    });
            }

            let k_arity = |k: &Continuation| match k {
                Continuation::Ref(ContinuationRef::To(k)) => continues.get(k).copied(),
                Continuation::Ref(_) => Some(Arity::AtLeast(0)),
                Continuation::Def(ContinuationDef { formals, .. }) => Some(Arity::from(formals)),
            };
            let get_arity = |x: &ValueID| io_lambdas.get(x).or_else(|| env.arities.get(x)).copied();
            match self.visit_mut(None, |_, e| {
                if let Apply { operator, operands, kontract, k, location } = e {
                    match operator {
                        Atom::Literal(_) => return ControlFlow::Break(Some(format!("application: not a procedure: {}:{}", atom_name(operator), location))),
                        Atom::Variable(x) => {
                            let expected = get_arity(x).map(|a| a.0).unwrap_or(Arity::Unknown);
                            let actual = Arity::from(&*operands);
                            if !expected.compatible_with(actual) {
                                return ControlFlow::Break(Some(format!("application: {}: arity mismatch; expected: {}, given: {}", atom_name(operator), expected, actual)));
                            }

                            if let Some(actual) = get_arity(x).map(|a| a.1) {
                                if let Some(expected) = k_arity(k) {
                                    if !expected.compatible_with(actual) {
                                        return ControlFlow::Break(Some(format!("application: {}: return value mismatch; expected: {}, given: {}", atom_name(operator), expected, actual)));
                                    }
                                }
                                *kontract = actual;
                            }
                        }
                        Atom::Core(primitive) => {
                            let expected = primitive.variants().iter().copied().reduce(Arity::join).unwrap_or(Arity::Exact(0));
                            let actual = Arity::from(&*operands);
                            if !expected.compatible_with(actual) {
                                return ControlFlow::Break(Some(format!("application: {}: arity mismatch; expected: {}, given: {}", atom_name(operator), expected, actual)));
                            }
                            let actual = primitive.returns();
                            if actual != Arity::Unknown { // all core primitives with unknown return-arity are optimized out of the CPS expression
                                if let Some(expected) = k_arity(k) {
                                    if !expected.compatible_with(actual) {
                                        return ControlFlow::Break(Some(format!("application: {}: return value mismatch; expected: {}, given: {}", atom_name(operator), expected, actual)));
                                    }
                                }
                                *kontract = actual;
                            }
                        }
                    }
                } else if let Continue { k, values } = e {
                    if let Some(expected) = k_arity(k) {
                        let actual = Arity::from(&*values);
                        if !expected.compatible_with(actual) {
                            return ControlFlow::Break(Some(format!("continuation: arity mismatch; expected: {}, given: {}", expected, actual)));
                        }
                    }
                } else if let Assign { vars, values, k } = e {
                    let expected = Arity::from(&*values);
                    let actual = Arity::from(&*vars);
                    if !expected.compatible_with(actual) {
                        return ControlFlow::Break(Some(format!("assignment: arity mismatch; expected: {}, given: {}", expected, actual)));
                    }
                    if let Some(expected) = k_arity(k) {
                        let actual = Arity::Exact(0);
                        if !expected.compatible_with(actual) {
                            return ControlFlow::Break(Some(format!("continuation: arity mismatch; expected: {}, given: {}", expected, actual)));
                        }
                    }
                }
                ControlFlow::Continue(None)
            }) {
                ControlFlow::Break(Some(e)) => Err(e),
                _ => Ok(io_lambdas),
            }
        }

        pub(super) fn optimize(&mut self, env: &Environment) -> bool {
            use Expression::*;
            match self {
                TakenOut => unreachable!(),
                LetLiteral { id, body, .. } if env.is_internal_binding(id) => {
                    let uses = body.uses_variable(*id);
                    if uses == 0 {
                        *self = std::mem::take(body);
                        true
                    } else if uses == 1 {
                        body.visit_mut(false, |modified, e| ControlFlow::Continue(match e {
                            Assign { .. } => e.hoist_assignment(id),
                            _ => modified,
                        })).continue_value().unwrap()
                    } else {
                        false
                    }
                },
                LetLambda { id, val, body } if env.is_internal_binding(id) => {
                    let uses = body.uses_variable(*id);
                    let applies = body.visit(0, |count, e| match e {
                        Apply { operator: Atom::Variable(operator), .. } if operator == id => ControlFlow::Continue(count + 1),
                        _ => ControlFlow::Continue(count),
                    }).continue_value().unwrap();
                    let is_recursive = val.body.visit((), |_, e| match e {
                        Apply { operator: Atom::Variable(operator), .. } if operator == id => ControlFlow::Break(()),
                        _ => ControlFlow::Continue(()),
                    }).is_break();
                    if uses == 0 {
                        *self = std::mem::take(body);
                        true
                    } else if uses == 1 && applies == 0 {
                        body.visit_mut(false, |modified, e| ControlFlow::Continue(match e {
                            Assign { .. } => e.hoist_assignment(id),
                            _ => modified,
                        })).continue_value().unwrap()
                    } else if uses == applies && !is_recursive && (applies == 1 || val.body.complexity() < BETA_EXPANSION_COMPLEXITY_THRESHOLD) {
                        body.visit_mut((), |_, e| match e {
                            Apply { operator: Atom::Variable(operator), operands, k, .. } if operator == id => {
                                let mut n = *if applies == 1 { std::mem::take(&mut val.body) } else { val.body.clone() };
                                n.rename(&val.formals, &*operands);
                                n.recontinue(val.escape, std::mem::take(k));
                                *e = n;
                                ControlFlow::Continue(())
                            },
                            _ => ControlFlow::Continue(()),
                        }).continue_value().unwrap();
                        *self = std::mem::take(body);
                        true
                    } else {
                        false
                    }
                },
                LetContinuation { id, k, body } => {
                    let (uses, applies) = body.visit((0, 0), |(uses, applies), e| match e {
                        Apply { k: Continuation::Ref(ContinuationRef::To(k)), .. } if k == id => ControlFlow::Continue((uses + 1, applies)),
                        Continue { k: Continuation::Ref(ContinuationRef::To(k)), .. } if k == id => ControlFlow::Continue((uses + 1, applies + 1)),
                        Assign { k: Continuation::Ref(ContinuationRef::To(k)), .. } if k == id => ControlFlow::Continue((uses + 1, applies)),
                        _ => ControlFlow::Continue((uses, applies)),
                    }).continue_value().unwrap();

                    if uses == 0 {
                        *self = std::mem::take(body);
                        true
                    } else if uses == applies && k.body.complexity() < BETA_EXPANSION_COMPLEXITY_THRESHOLD {
                        body.visit_mut((), |_, e| match e {
                            Continue { k: Continuation::Ref(ContinuationRef::To(k_id)), values } if k_id == id => {
                                let mut n = *if applies == 1 { std::mem::take(&mut k.body) } else { k.body.clone() };
                                n.rename(&k.formals, &*values);
                                *e = n;
                                ControlFlow::Continue(())
                            },
                            _ => ControlFlow::Continue(()),
                        }).continue_value().unwrap();
                        *self = std::mem::take(body);
                        true
                    } else {
                        false
                    }
                }
                Apply { operator, operands: (operands, operands_tail), kontract, k, location } => {
                    match operator {
                        Atom::Core(frontend::CorePrimitive::Apply) => {
                            *self = Apply {
                                operator: *operands.first().unwrap(),
                                operands: (operands.split_off(1), *operands_tail),
                                location: *location,
                                kontract: *kontract,
                                k: std::mem::take(k),
                            };
                            true
                        },
                        Atom::Core(frontend::CorePrimitive::CallWithCurrentContinuation) => {
                            let stored_dynamic_extent = env.new_id();
                            let reified_cont = env.new_id();
                            let values = env.new_id();
                            *self = env.atomize_continuation(std::mem::take(k), |k| Apply {
                                operator: Atom::Core(frontend::CorePrimitive::StoreDynamicExtent),
                                operands: (vec![], None),
                                location: Default::default(),
                                kontract: Arity::Exact(1),
                                k: Continuation::Def(ContinuationDef {
                                    formals: (vec![stored_dynamic_extent], None),
                                    body: Box::new(LetLambda {
                                        id: reified_cont,
                                        val: Lambda {
                                            formals: (vec![], Some(values)),
                                            escape: ContinuationRef::Escape,
                                            body: Box::new(Apply {
                                                operator: Atom::Core(frontend::CorePrimitive::RewindDynamicExtent),
                                                operands: (vec![Atom::Variable(stored_dynamic_extent)], None),
                                                location: Default::default(),
                                                kontract: Arity::Exact(0),
                                                k: Continuation::Def(ContinuationDef {
                                                    formals: (vec![], None),
                                                    body: Box::new(Continue { k: Continuation::Ref(k), values: (vec![], Some(values)) }),
                                                }),
                                            }),
                                        },
                                        body: Box::new(Apply {
                                            operator: *operands.first().unwrap(),
                                            operands: (vec![Atom::Variable(reified_cont)], None),
                                            location: *location,
                                            kontract: *kontract,
                                            k: Continuation::Ref(k),
                                        }),
                                    }),
                                }),
                            });
                            true
                        },
                        Atom::Core(frontend::CorePrimitive::CallWithValues) => {
                            let values = env.new_id();
                            *self = Apply {
                                operator: *operands.get(0).unwrap(),
                                operands: (vec![], None),
                                location: *location,
                                kontract: Arity::Unknown,
                                k: Continuation::Def(ContinuationDef {
                                    formals: (vec![], Some(values)),
                                    body: Box::new(Apply {
                                        operator: *operands.get(1).unwrap(),
                                        operands: (vec![], Some(values)),
                                        location: *location,
                                        kontract: Arity::AtLeast(0),
                                        k: std::mem::take(k),
                                    }),
                                }),
                            };
                            true
                        },
                        Atom::Core(frontend::CorePrimitive::DynamicWind) => {
                            let stored_dynamic_extent = env.new_id();
                            let thunk_values = env.new_id();
                            *self = Apply {
                                operator: *operands.get(0).unwrap(),
                                operands: (vec![], None),
                                location: *location,
                                kontract: Arity::Exact(0),
                                k: Continuation::Def(ContinuationDef {
                                    formals: (vec![], None),
                                    body: Box::new(Apply {
                                        operator: Atom::Core(frontend::CorePrimitive::PushDynamicFrame),
                                        operands: (vec![*operands.get(0).unwrap(), *operands.get(2).unwrap()], None),
                                        location: *location,
                                        kontract: Arity::Exact(1),
                                        k: Continuation::Def(ContinuationDef {
                                            formals: (vec![stored_dynamic_extent], None),
                                            body: Box::new(Apply {
                                                operator: *operands.get(1).unwrap(),
                                                operands: (vec![], None),
                                                location: *location,
                                                kontract: Arity::AtLeast(0),
                                                k: Continuation::Def(ContinuationDef {
                                                    formals: (vec![], Some(thunk_values)),
                                                    body: Box::new(Apply {
                                                        operator: Atom::Core(frontend::CorePrimitive::RewindDynamicExtent),
                                                        operands: (vec![Atom::Variable(stored_dynamic_extent)], None),
                                                        location: *location,
                                                        kontract: Arity::Exact(0),
                                                        k: Continuation::Def(ContinuationDef {
                                                            formals: (vec![], None),
                                                            body: Box::new(Continue {
                                                                k: std::mem::take(k),
                                                                values: (vec![], Some(thunk_values)),
                                                            }),
                                                        }),
                                                    }),
                                                }),
                                            }),
                                        }),
                                    }),
                                }),
                            };
                            true
                        },
                        Atom::Core(frontend::CorePrimitive::Values) => {
                            *self = Continue {
                                k: std::mem::take(k),
                                values: (std::mem::take(operands), *operands_tail),
                            };
                            true
                        },
                        Atom::Core(frontend::CorePrimitive::Raise) => {
                            let eh = env.new_id();
                            let old_extent = env.new_id();
                            *self = Apply {
                                operator: Atom::Core(frontend::CorePrimitive::FindExceptionHandler),
                                operands: (vec![], None),
                                location: Default::default(),
                                kontract: Arity::Exact(2),
                                k: Continuation::Def(ContinuationDef {
                                    formals: (vec![eh, old_extent], None),
                                    body: Box::new(Apply {
                                        operator: Atom::Core(frontend::CorePrimitive::RewindDynamicExtent),
                                        operands: (vec![Atom::Variable(old_extent)], None),
                                        location: Default::default(),
                                        kontract: Arity::Exact(0),
                                        k: Continuation::Def(ContinuationDef {
                                            formals: (vec![], None),
                                            body: Box::new(Apply {
                                                operator: Atom::Variable(eh),
                                                operands: (std::mem::take(operands), *operands_tail),
                                                location: *location,
                                                kontract: Arity::Unknown,
                                                k: Continuation::Ref(ContinuationRef::Abort(std::num::NonZero::new(1).unwrap())),
                                            }),
                                        }),
                                    }),
                                }),
                            };
                            true
                        },
                        Atom::Core(frontend::CorePrimitive::RaiseContinuable) => {
                            let eh = env.new_id();
                            *self = Apply {
                                operator: Atom::Core(frontend::CorePrimitive::FindExceptionHandler),
                                operands: (vec![], None),
                                location: Default::default(),
                                kontract: Arity::Exact(2),
                                k: Continuation::Def(ContinuationDef {
                                    formals: (vec![eh, ValueID::invalid()], None),
                                    body: Box::new(Apply {
                                        operator: Atom::Variable(eh),
                                        operands: (std::mem::take(operands), *operands_tail),
                                        location: *location,
                                        kontract: Arity::AtLeast(0),
                                        k: std::mem::take(k),
                                    }),
                                }),
                            };
                            true
                        },
                        Atom::Core(frontend::CorePrimitive::WithExceptionHandler) => {
                            let dynamic_extent = env.new_id();
                            let thunk_values = env.new_id();
                            *self = Apply {
                                operator: Atom::Core(frontend::CorePrimitive::PushExceptionFrame),
                                operands: (vec![*operands.get(0).unwrap()], None),
                                location: Default::default(),
                                kontract: Arity::Exact(1),
                                k: Continuation::Def(ContinuationDef {
                                    formals: (vec![dynamic_extent], None),
                                    body: Box::new(Apply {
                                        operator: *operands.get(1).unwrap(),
                                        operands: (vec![], None),
                                        location: *location,
                                        kontract: Arity::Unknown,
                                        k: Continuation::Def(ContinuationDef {
                                            formals: (vec![], Some(thunk_values)),
                                            body: Box::new(Apply {
                                                operator: Atom::Core(frontend::CorePrimitive::RewindDynamicExtent),
                                                operands: (vec![Atom::Variable(dynamic_extent)], None),
                                                location: Default::default(),
                                                kontract: Arity::Exact(0),
                                                k: Continuation::Def(ContinuationDef {
                                                    formals: (vec![], None),
                                                    body: Box::new(Continue {
                                                        k: std::mem::take(k),
                                                        values: (vec![], Some(thunk_values)),
                                                    }),
                                                }),
                                            }),
                                        }),
                                    }),
                                }),
                            };
                            true
                        },
                        _ => false,
                    }
                }
                Branch { test, consequent, alternate } => match test {
                    Atom::Literal(LiteralC::Boolean(false)) => {
                        *self = std::mem::take(alternate);
                        true
                    },
                    Atom::Literal(_) | Atom::Core(_) => {
                        *self = std::mem::take(consequent);
                        true
                    },
                    _ => false,
                },
                Continue { k: Continuation::Def(ContinuationDef { formals, body }), values } => {
                    body.rename(formals, &*values);
                    *self = std::mem::take(body);
                    true
                },
                Assign { vars, k, .. } if vars.0.is_empty() && vars.1.is_none() => {
                    *self = Continue { k: std::mem::take(k), values: (vec![], None) };
                    true
                },
                _ => false,
            }
        }
    }
}

pub mod anf {
    use crate::compiler::{Literal, LiteralRef, LiteralC, LiteralD};
    use super::*;
    const BETA_EXPANSION_COMPLEXITY_THRESHOLD: usize = 11;

    fn format_varargs<T: std::fmt::Display, U: Copy + std::fmt::Display>(formals: &Vec<T>, tail: Option<U>) -> String {
        let formals0 = formals.iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>().join(" ");
        if let Some(tail) = tail {
            if formals0.len() > 0 {
                format!("({} . {})", formals0, tail)
            } else {
                tail.to_string()
            }
        } else {
            format!("({})", formals0)
        }
    }

    #[derive(Debug, Clone)]
    pub struct Lambda {
        pub(crate) formals: (Vec<ValueID>, Option<ValueID>),
        pub(crate) closed_env: Vec<ValueID>,
        pub(crate) body: Box<Expression>,
        pub(crate) returns: Arity,
    }
    impl std::fmt::Display for Lambda {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            writeln!(f, "[\u{03BB} {} closed:{}] -> {} {{", format_varargs(&self.formals.0, self.formals.1), format_varargs(&self.closed_env, None::<u8>), self.returns)?;
            for line in self.body.to_string().lines() {
                writeln!(f, "    {}", line)?;
            }
            writeln!(f, "}}")
        }
    }

    #[derive(Debug, Clone)]
    pub enum Rhs {
        Literal(Rc<LiteralD>),
        Lambda(Lambda),
        Closure { index: usize, closed_env: Vec<Atom> },
    }
    #[derive(Debug, Clone)]
    pub enum Rhses {
        Values { values: (Vec<Atom>, Option<ValueID>) },
        Apply { operator: Atom, operands: (Vec<Atom>, Option<ValueID>) },
    }
    impl std::fmt::Display for Rhs {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Rhs::Literal(x) => match x.as_ref() {
                    LiteralD::Static(_) | LiteralD::Symbol(_) | LiteralD::List(..) => write!(f, "'{}", x.as_ref()),
                    _ => std::fmt::Display::fmt(x.as_ref(), f),
                },
                Rhs::Lambda(_) => write!(f, "\u{03BB}"),
                Rhs::Closure { index, closed_env } => write!(f, "closure#{}[{}]", index, format_varargs(closed_env, None::<u8>)),
            }
        }
    }
    impl std::fmt::Display for Rhses {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Rhses::Values { values } => std::fmt::Display::fmt(&format_varargs(&values.0, values.1), f),
                Rhses::Apply { operator, operands } => write!(f, "call [{}] with {}", operator, format_varargs(&operands.0, operands.1)),
            }
        }
    }

    #[derive(Debug, Clone, Default)]
    pub enum Expression {
        #[default] Nop,
        Let { id: ValueID, val: Rhs, body: Box<Expression> },
        LetValues { id: (Vec<ValueID>, Option<ValueID>), val: Rhses, body: Box<Expression> },
        Branch { test: Atom, consequent: Box<Expression>, alternate: Box<Expression> },
        Return { values: (Vec<Atom>, Option<ValueID>) },
        TailCall { to: ValueID, values: (Vec<Atom>, Option<ValueID>) },
        Halt(u8),
    }
    impl std::fmt::Display for Expression {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            use Expression::*;
            match self {
                Nop => write!(f, "nop"),
                Let { id, val, body } => {
                    writeln!(f, "let {} = {}", id, val)?;
                    std::fmt::Display::fmt(body, f)
                },
                LetValues { id, val, body } => {
                    writeln!(f, "let {} = {}", format_varargs(&id.0, id.1), val)?;
                    std::fmt::Display::fmt(body, f)
                }
                Branch { test, consequent, alternate } => {
                    writeln!(f, "if {} {{", test)?;
                    for line in consequent.to_string().lines() {
                        writeln!(f, "    {}", line)?;
                    }
                    writeln!(f, "}} else {{")?;
                    for line in alternate.to_string().lines() {
                        writeln!(f, "    {}", line)?;
                    }
                    writeln!(f, "}}")
                }
                Return { values } => writeln!(f, "ret {}", format_varargs(&values.0, values.1)),
                TailCall { to, values } => writeln!(f, "tailcall [{}] with {}", to, format_varargs(&values.0, values.1)),
                Halt(n) => writeln!(f, "hlt {}", n),
            }
        }
    }
    impl Expression {
        pub(super) fn visit<T>(&self, init: T, mut f: impl FnMut(T, &Expression) -> ControlFlow<T, T>) -> ControlFlow<T, T> {
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
                    Return { .. } | TailCall { .. } | Nop | Halt(_) => ControlFlow::Continue(init),
                }
            }

            let r = visit_children(self, init, &mut f)?;
            f(r, self)
        }
        pub(super) fn visit_mut<T>(&mut self, init: T, mut f: impl FnMut(T, &mut Expression) -> ControlFlow<T, T>) -> ControlFlow<T, T> {
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
                    Return { values } => {
                        values.0.iter()
                            .filter_map(|a| if let Atom::Variable(a) = a { Some(*a) } else { None })
                            .chain(values.1.iter().copied())
                            .filter(|&i| i == target).count()
                    },
                    TailCall { to, values, .. } => {
                        values.0.iter()
                            .filter_map(|a| if let Atom::Variable(a) = a { Some(*a) } else { None })
                            .chain(values.1.iter().copied())
                            .chain(std::iter::once(*to))
                            .filter(|&i| i == target).count()
                    },
                    Nop | Halt(_) => 0,
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
                    LetValues { id, val, .. } => {
                        intros.extend(id.0.iter().chain(id.1.iter()).copied());
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
                    Nop | Halt(_) => (),
                }
                ControlFlow::Continue((intros, uses))
            }).continue_value().unwrap()
        }
        pub(super) fn return_arity(&self, env: &Environment) -> Arity {
            use Expression::*;
            self.visit(None, |arity: Option<Arity>, e| {
                if let Return { values } = e {
                    ControlFlow::Continue(match arity {
                        None => Some(Arity::from(values)),
                        Some(arity) => Some(arity.join(Arity::from(values))),
                    })
                } else if let TailCall { to, .. } = e {
                    if let Some(tailcall_arity) = env.arities.get(to).map(|a| a.1) {
                        ControlFlow::Continue(match arity {
                            None => Some(tailcall_arity),
                            Some(arity) => Some(arity.join(tailcall_arity)),
                        })
                    } else {
                        ControlFlow::Continue(arity)
                    }
                } else {
                    ControlFlow::Continue(arity)
                }
            }).continue_value().unwrap().unwrap_or(Arity::Exact(0))
        }

        pub(super) fn rename(&mut self, from: &(Vec<ValueID>, Option<ValueID>), to: &(Vec<Atom>, Option<ValueID>), env: &Environment) -> bool {
            assert!(from.0.len() <= to.0.len());
            let mapping = std::iter::zip(from.0.iter().copied(), to.0.iter().copied()).collect::<std::collections::HashMap<_, _>>();
            let tail_mapping = (&to.0[from.0.len() ..], to.1);
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
                    TailCall { to, values } => {
                        let mapped_values = map_atoms(values);
                        match mapping.get(to) {
                            Some(Atom::Literal(_)) => panic!(),
                            Some(Atom::Variable(mapped)) => {
                                *to = *mapped;
                                true
                            },
                            Some(Atom::Core(primitive)) => {
                                let id = env.new_ids(primitive.returns());
                                let body = Return { values: (id.0.iter().copied().map(Atom::Variable).collect(), id.1) };
                                *e = LetValues {
                                    id,
                                    val: Rhses::Apply { operator: Atom::Core(*primitive), operands: std::mem::take(values) },
                                    body: Box::new(body),
                                };
                                true
                            },
                            None => mapped_values,
                        }
                    }
                    Nop | Halt(_) => unchanged,
                })
            }).continue_value().unwrap()
        }

        pub(super) fn optimize(&mut self, env: &Environment) -> bool {
            use Expression::*;
            match self {
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
                    if env.is_internal_binding(lambda_id) == false {
                        false
                    } else if uses == 0 {
                        *self = std::mem::take(body);
                        true
                    } else if uses == applies && !is_recursive && (applies == 1 || lambda.body.complexity() < BETA_EXPANSION_COMPLEXITY_THRESHOLD) {
                        body.visit_mut((), |_, e| match e {
                            LetValues { id, val: Rhses::Apply { operator: Atom::Variable(operator), operands }, body } if lambda_id == operator => {
                                let mut n = *if applies == 1 { std::mem::take(&mut lambda.body) } else { lambda.body.clone() };
                                n.rename(&lambda.formals, &operands, env);
                                n.visit_mut((), |_, e| {
                                    if let Return { values } = e {
                                        *e = LetValues { id: id.clone(), val: Rhses::Values { values: values.clone() }, body: body.clone() };
                                    }
                                    ControlFlow::Continue(())
                                }).continue_value().unwrap();
                                *e = n;
                                ControlFlow::Continue(())
                            },
                            TailCall { to, values } if lambda_id == to => {
                                let mut n = *if applies == 1 { std::mem::take(&mut lambda.body) } else { lambda.body.clone() };
                                n.rename(&lambda.formals, &values, env);
                                *e = n;
                                ControlFlow::Continue(())
                            },
                            _ => ControlFlow::Continue(()),
                        }).continue_value().unwrap();
                        *self = std::mem::take(body);
                        true
                    } else {
                        false
                    }
                },
                Let { val: Rhs::Closure { .. }, .. } => unreachable!(),
                LetValues { id, val: Rhses::Values { values }, body } => {
                    if id.0.iter().chain(id.1.iter()).all(|id| env.is_internal_binding(id)) {
                        body.rename(&id, &values, env);
                        *self = std::mem::take(body);
                        true
                    } else {
                        false
                    }
                },
                LetValues { id: (id, tail_id), val: Rhses::Apply { operator, operands }, body } => {
                    if let Atom::Core(operator) = operator {
                        if let (operands, None) = operands {
                            if let Some(Some(returns)) = operands.iter().map(|atom| match atom {
                                Atom::Literal(x) => Ok(LiteralRef::Copy(*x)),
                                Atom::Variable(x) => env.literals.get(x).map(|e| LiteralRef::NoCopy(e.as_ref())).ok_or(()),
                                Atom::Core(_) => Err(())
                            }).collect::<Result<Vec<_>, _>>().ok().map(|operands| Self::constant_fold(*operator, operands)) {
                                assert_eq!(operator.returns(), Arity::Exact(returns.len()));
                                let tail_cs = returns.iter().rev()
                                    .take_while(|x| matches!(x, Literal::Copy(_)))
                                    .count(); // The number of LiteralC's at the tail of returns
                                let inner_body = if returns.len() > id.len() + tail_cs {
                                    // This will only be true if tail_id exists and captures some LiteralD.
                                    // If tail_id is None, the arity checker enforces that returns.len() <= id.len() + tail_cs for all values of tail_cs.
                                    // Generate temporaries for any captured LiteralD, and then assign the whole set of temporaries to the old tail_id.
                                    let temps = env.new_ids(Arity::AtLeast(returns.len() - (id.len() + tail_cs)));
                                    let init = LetValues {
                                        id: (vec![], *tail_id),
                                        val: Rhses::Values { values: (temps.0.iter().copied().map(Atom::Variable).collect(), temps.1) },
                                        body: std::mem::take(body),
                                    };
                                    *tail_id = temps.1;
                                    id.extend(temps.0.into_iter());
                                    Box::new(init)
                                } else {
                                    std::mem::take(body)
                                };

                                // Assign all LiteralC in one LetValues, since they're atomic.
                                let inner = {
                                    let mut inner_id = Vec::with_capacity(returns.len());
                                    let mut inner_val = Vec::with_capacity(returns.len());
                                    for (id, r) in std::iter::zip(id.iter().copied(), returns.iter()) {
                                        if let Literal::Copy(x) = r {
                                            inner_id.push(id);
                                            inner_val.push(Atom::Literal(*x));
                                        }
                                    }
                                    inner_val.extend(returns.iter().skip(id.len()).map(|x| match x {
                                        Literal::Copy(x) => Atom::Literal(*x),
                                        Literal::NoCopy(_) => panic!(),
                                    }));
                                    LetValues { id: (inner_id, *tail_id), val: Rhses::Values { values: (inner_val, None) }, body: inner_body }
                                };

                                // Hoist all LiteralD into their own separate Let's.
                                *self = std::iter::zip(id.iter().copied(), returns.into_iter()).rev().fold(inner, |e, (id, r)| match r {
                                    Literal::Copy(_) => e,
                                    Literal::NoCopy(r) => Let { id, val: Rhs::Literal(Rc::new(r)), body: Box::new(e) },
                                });
                                return true;
                            }
                        }
                    }
                    if let Return { values: (values, tail_value) } = &**body {
                        if let Atom::Variable(to) = *operator {
                            if std::iter::zip(id.iter().copied(), values.iter().copied()).all(|(i, a)| a == Atom::Variable(i)) && tail_id == tail_value {
                                *self = TailCall { to, values: std::mem::take(operands) };
                                return true;
                            }
                        }
                    }
                    false
                },
                _ => false,
            }
        }
        fn constant_fold(operator: frontend::CorePrimitive, operands: Vec<LiteralRef>) -> Option<Vec<Literal>> {
            use LiteralRef::*;
            use LiteralC::*;
            match operator {
                frontend::CorePrimitive::Sum => {
                    if operands.iter().all(|op| matches!(op, Copy(Integer(_)))) {
                        let operands = operands.iter().map(|op| {
                            let Copy(Integer(op)) = op else { panic!() };
                            *op
                        });
                        let sum = operands.reduce(|acc, e| acc + e).unwrap_or(0);
                        Some(vec![Literal::Copy(Integer(sum))])
                    } else {
                        None
                    }
                },
                _ => None,
            }
        }

        pub(super) fn to_closures(mut self, env: &Environment) -> Vec<Lambda> {
            let mut lambdas = self.visit_mut(Vec::new(), |mut lambdas, e| {
                use Expression::*;
                if let Let { id, val: Rhs::Lambda(lambda), body } = e {
                    let (mut intros, uses) = lambda.body.variables();
                    intros.extend(lambda.formals.0.iter().chain(lambda.formals.1.iter()).copied());

                    let index = lambdas.len();
                    let closed_env = uses.difference(&intros).copied().collect::<Vec<_>>();
                    let converted = Let {
                        id: *id,
                        val: Rhs::Closure { index, closed_env: closed_env.iter().copied().map(Atom::Variable).collect() },
                        body: std::mem::take(body),
                    };
                    lambdas.push(Lambda {
                        formals: std::mem::take(&mut lambda.formals),
                        closed_env,
                        body: std::mem::take(&mut lambda.body),
                        returns: lambda.returns,
                    });
                    *e = converted;
                }
                ControlFlow::Continue(lambdas)
            }).continue_value().unwrap();

            let root_returns = self.return_arity(env);
            let (intros, uses) = self.variables();
            lambdas.push(Lambda {
                formals: (vec![], None),
                closed_env: uses.difference(&intros).copied().collect(),
                body: Box::new(self),
                returns: root_returns,
            });
            lambdas
        }
    }
}

struct Environment<'p> {
    bound_variables: HashSet<ValueID>,
    current_return: std::cell::Cell<ContinuationRef>,
    parent_env: &'p mut frontend::Environment,
}
impl<'p> From<&'p mut frontend::Environment> for Environment<'p> {
    fn from(parent_env: &'p mut frontend::Environment) -> Self {
        Self {
            bound_variables: parent_env.toplevels.values().filter_map(|b| match b {
                Binding::Variable(i) => Some(ValueID(*i)),
                _ => None,
            }).collect(),
            current_return: std::cell::Cell::new(ContinuationRef::Escape),
            parent_env,
        }
    }
}
impl std::ops::Deref for Environment<'_> {
    type Target = frontend::Environment;

    fn deref(&self) -> &Self::Target {
        self.parent_env
    }
}
impl std::ops::DerefMut for Environment<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.parent_env
    }
}
impl<'p> Environment<'p> {
    fn new_id(&self) -> ValueID {
        ValueID(self.parent_env.new_id())
    }
    fn new_continuation_id(&self) -> ContinuationID {
        ContinuationID(self.parent_env.new_id())
    }
    fn new_ids(&self, arity: Arity) -> (Vec<ValueID>, Option<ValueID>) {
        let arity = match arity {
            Arity::Exact(n) => (n, None),
            Arity::AtLeast(n) => (n, Some(self.new_id())),
            Arity::Unknown => (0, Some(self.new_id())),
        };
        ((0..arity.0).map(|_| self.new_id()).collect(), arity.1)
    }
    fn is_escape(&self, k: &Continuation) -> bool {
        match k {
            Continuation::Ref(ContinuationRef::Escape) => true,
            Continuation::Ref(k) => *k == self.current_return.get(),
            _ => false,
        }
    }
    fn is_internal_binding(&self, id: &ValueID) -> bool {
        self.bound_variables.contains(id) == false
    }
    fn with_return<R>(&self, k: ContinuationRef, f: impl FnOnce(&Self) -> R) -> R {
        let old = self.current_return.replace(k);
        let r = f(self);
        self.current_return.set(old);
        r
    }

    fn atomize(&self, e: Located<syntax::Expression>, then: impl FnOnce(Atom) -> cps::Expression) -> cps::Expression {
        use syntax::Expression as stx;
        match *e {
            stx::Literal(super::Literal::Copy(x)) => then(Atom::Literal(x)),
            stx::Variable(i) => then(Atom::Variable(ValueID(i))),
            stx::Core(s) => then(Atom::Core(s)),
            _ => {
                let x = self.new_id();
                let k = then(Atom::Variable(x));
                self.to_cps(e, Continuation::Def(ContinuationDef { formals: (vec![x], None), body: Box::new(k) }))
            },
        }
    }
    fn atomize_many(&self, ee: Vec<Located<syntax::Expression>>, then: impl FnOnce(Vec<Atom>) -> cps::Expression) -> cps::Expression {
        use syntax::Expression as stx;
        let (atoms, new_ids) = ee.iter().map(|e| match &**e {
            stx::Literal(super::Literal::Copy(x)) => (Atom::Literal(*x), None),
            stx::Variable(i) => (Atom::Variable(ValueID(*i)), None),
            stx::Core(s) => (Atom::Core(*s), None),
            _ => {
                let x = self.new_id();
                (Atom::Variable(x), Some(x))
            },
        }).collect::<(Vec<_>, Vec<_>)>();
        std::iter::zip(new_ids.into_iter(), ee.into_iter()).rev()
            .fold(then(atoms), |k, (id, e)| match id {
                None => k,
                Some(x) => self.to_cps(e, Continuation::Def(ContinuationDef {
                    formals: (vec![x], None),
                    body: Box::new(k),
                })),
            })
    }
    fn atomize_continuation(&self, k: Continuation, then: impl FnOnce(ContinuationRef) -> cps::Expression) -> cps::Expression {
        match k {
            Continuation::Ref(k) => then(k),
            Continuation::Def(k) => {
                let id = self.new_continuation_id();
                cps::Expression::LetContinuation { id, k, body: Box::new(then(ContinuationRef::To(id))) }
            }
        }
    }

    fn to_cps(&self, e: Located<syntax::Expression>, k: Continuation) -> cps::Expression {
        use syntax::Expression as stx;
        use cps::Expression::*;
        let Located { item: e, location } = e;
        match e {
            stx::Literal(super::Literal::Copy(x)) => Continue { k, values: (vec![Atom::Literal(x)], None) },
            stx::Literal(super::Literal::NoCopy(val)) => {
                let id = self.new_id();
                LetLiteral {
                    id,
                    val: Rc::new(val),
                    body: Box::new(Continue { k, values: (vec![Atom::Variable(id)], None) }),
                }
            },
            stx::Variable(i) => Continue { k, values: (vec![Atom::Variable(ValueID(i))], None) },
            stx::Core(s) => Continue { k, values: (vec![Atom::Core(s)], None) },
            stx::Call { operator, operands } => {
                self.atomize(*operator, move |operator|
                    self.atomize_many(operands, move |operands|
                        Apply { operator, operands: (operands, None), kontract: Arity::Unknown, k, location }))
            },
            stx::Lambda { formals, body } => {
                let id = self.new_id();
                let formals = (formals.0.iter().map(|e| ValueID(**e)).collect(), formals.1.map(|e| ValueID(*e)));
                let escape = self.new_continuation_id();
                let body = self.block_to_cps(body, Continuation::from(escape));
                LetLambda {
                    id,
                    val: cps::Lambda {
                        formals,
                        escape: ContinuationRef::To(escape),
                        body: Box::new(body),
                    },
                    body: Box::new(Continue { k, values: (vec![Atom::Variable(id)], None) })
                }
            },
            stx::Conditional { test, consequent, alternate } => {
                self.atomize_continuation(k, move |k|
                    self.atomize(*test, move |test| Branch {
                        test,
                        consequent: Box::new(self.to_cps(*consequent, Continuation::Ref(k))),
                        alternate: Box::new(match alternate {
                            Some(alternate) => self.to_cps(*alternate, Continuation::Ref(k)),
                            None => Continue { k: Continuation::Ref(k), values: (vec![Atom::Literal(super::LiteralC::Boolean(false))], None) }
                        }),
                    }))
            }
            stx::Assign { ids, values } => {
                let all = self.new_id();
                self.to_cps(*values, Continuation::Def(ContinuationDef {
                    formals: (vec![], Some(all)),
                    body: Box::new(Assign {
                        vars: (ids.0.iter().map(|e| ValueID(**e)).collect(), ids.1.map(|e| ValueID(*e))),
                        values: (vec![], Some(all)),
                        k,
                    })
                }))
            },
            stx::Block { body } => self.block_to_cps(body, k),
        }
    }
    fn block_to_cps(&self, e: Vec<Located<syntax::Expression>>, k: Continuation) -> cps::Expression {
        let mut ee = e.into_iter().rev();
        if let Some(tail) = ee.next() {
            ee.fold(self.to_cps(tail, k), |acc, e|
                self.to_cps(e, Continuation::Def(ContinuationDef { formals: (vec![], Some(ValueID::invalid())), body: Box::new(acc) })))
        } else {
            cps::Expression::Continue { k, values: (vec![], None) }
        }
    }
    fn normalize_cps(&self, e: cps::Expression) -> anf::Expression {
        use cps::Expression as CPS;
        use anf::Expression as ANF;
        use anf::{Rhs, Rhses};
        match e {
            CPS::LetLiteral { id, val, body } => ANF::Let {
                id,
                val: Rhs::Literal(val),
                body: Box::new(self.normalize_cps(*body)),
            },
            CPS::LetLambda { id, val, body } => {
                let lambda_body = self.with_return(val.escape, |env| env.normalize_cps(*val.body));
                let returns = lambda_body.return_arity(self);
                ANF::Let {
                    id,
                    val: Rhs::Lambda(anf::Lambda {
                        formals: val.formals,
                        closed_env: vec![],
                        body: Box::new(lambda_body),
                        returns,
                    }),
                    body: Box::new(self.normalize_cps(*body)),
                }
            },
            CPS::LetContinuation { id, k, body } => ANF::Let {
                id: ValueID(id.0),
                val: Rhs::Lambda(anf::Lambda {
                    formals: k.formals,
                    closed_env: vec![],
                    body: Box::new(self.normalize_cps(*k.body)),
                    returns: Arity::Exact(0),
                }),
                body: Box::new(self.normalize_cps(*body)),
            },
            CPS::Apply { operator, operands, location: _, kontract, k } => {
                if self.is_escape(&k) {
                    let id = self.new_ids(kontract);
                    let values = (id.0.iter().copied().map(Atom::Variable).collect(), id.1);
                    ANF::LetValues {
                        id,
                        val: Rhses::Apply { operator, operands },
                        body: Box::new(ANF::Return { values }),
                    }
                } else {
                    match k {
                        Continuation::Def(ContinuationDef { formals, body }) => ANF::LetValues {
                            id: formals,
                            val: Rhses::Apply { operator, operands },
                            body: Box::new(self.normalize_cps(*body)),
                        },
                        Continuation::Ref(ContinuationRef::To(k)) => {
                            let id = self.new_ids(kontract);
                            let values = (id.0.iter().copied().map(Atom::Variable).collect(), id.1);
                            ANF::LetValues {
                                id,
                                val: Rhses::Apply { operator, operands },
                                body: Box::new(ANF::TailCall { to: ValueID(k.0), values }),
                            }
                        },
                        Continuation::Ref(ContinuationRef::Abort(n)) => ANF::Halt(n.get()),
                        Continuation::Ref(ContinuationRef::Escape) => unreachable!(),
                    }
                }
            },
            CPS::Branch { test, consequent, alternate } => ANF::Branch {
                test,
                consequent: Box::new(self.normalize_cps(*consequent)),
                alternate: Box::new(self.normalize_cps(*alternate)),
            },
            CPS::Continue { k, values } => {
                if self.is_escape(&k) {
                    ANF::Return { values }
                } else {
                    match k {
                        Continuation::Def(ContinuationDef { formals, body }) => {
                            let mut body = self.normalize_cps(*body);
                            body.rename(&formals, &values, self);
                            body
                        },
                        Continuation::Ref(ContinuationRef::To(k)) => ANF::TailCall { to: ValueID(k.0), values },
                        Continuation::Ref(ContinuationRef::Abort(n)) => ANF::Halt(n.get()),
                        Continuation::Ref(ContinuationRef::Escape) => unreachable!(),
                    }
                }
            },
            CPS::Assign { vars, values, k } => ANF::LetValues {
                id: vars,
                val: Rhses::Values { values },
                body: Box::new(if self.is_escape(&k) {
                    ANF::Return { values: (vec![], None) }
                } else {
                    match k {
                        Continuation::Def(ContinuationDef { formals: _, body }) => self.normalize_cps(*body), // rename is unnecessary since there are no formals
                        Continuation::Ref(ContinuationRef::To(k)) => ANF::TailCall { to: ValueID(k.0), values: (vec![], None) },
                        Continuation::Ref(ContinuationRef::Abort(n)) => ANF::Halt(n.get()),
                        Continuation::Ref(ContinuationRef::Escape) => unreachable!(),
                    }
                }),
            },
            CPS::TakenOut => panic!(),
        }
    }
}

pub fn normalize(e: Located<syntax::Expression>, env: &mut frontend::Environment) -> Result<Vec<anf::Lambda>, String> {
    let mut env = Environment::from(env);
    let mut e = env.to_cps(e, Continuation::Ref(ContinuationRef::Escape));
    e.check_arity(&env)?;

    while e.visit_mut((), |_, e| match e.optimize(&env) {
        true => ControlFlow::Break(()),
        false => ControlFlow::Continue(())
    }).is_break() { }

    let arity_extension = e.check_arity(&env)?;
    env.arities.extend(arity_extension);
    env.literals.extend(e.visit(HashMap::new(), |mut literals, e| {
        if let cps::Expression::LetLiteral { id, val, .. } = e {
            literals.insert(*id, Rc::clone(val));
        }
        ControlFlow::Continue(literals)
    }).continue_value().unwrap());
    eprintln!("cps: {:?}", e);

    let mut e = env.normalize_cps(e);
    while e.visit_mut((), |_, e| match e.optimize(&env) {
        true => ControlFlow::Break(()),
        false => ControlFlow::Continue(()),
    }).is_break() { }
    eprintln!("anf: {:?}", e);

    Ok(e.to_closures(&env))
}
