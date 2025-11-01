use std::borrow::Cow;
use std::collections::HashMap;
use strum::{EnumIter, FromRepr, IntoEnumIterator, IntoStaticStr};
use super::{Arity, Binding, ValueID};
use super::syntax;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, EnumIter, IntoStaticStr)]
pub enum CoreForm {
    #[strum(serialize = "lambda")] Lambda,
    #[strum(serialize = "if")] If,
    #[strum(serialize = "begin")] Begin,
    #[strum(serialize = "set-values!")] SetValues,
    #[strum(serialize = "quote")] Quote,
    #[strum(serialize = "quasiquote")] Quasiquote,
    #[strum(serialize = "unquote")] Unquote,
    #[strum(serialize = "unquote-splicing")] UnquoteSplicing,
    #[strum(serialize = "define-values")] DefineValues,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, EnumIter, FromRepr, IntoStaticStr)]
#[repr(u32)]
pub enum CorePrimitive {
    // Addressable (bound)
    #[strum(serialize = "zero?")] Zero_,
    #[strum(serialize = "eqv?")] Eqv_,
    #[strum(serialize = "eq?")] Eq_,
    #[strum(serialize = "equal?")] Equal_,

    #[strum(serialize = "number?")] IsNumber_,
    #[strum(serialize = "exact?")] Exact_,
    #[strum(serialize = "inexact?")] Inexact_,
    #[strum(serialize = "=")] Numequal,
    #[strum(serialize = "<")] NumLt,
    #[strum(serialize = "even?")] Even_,
    #[strum(serialize = "+")] Sum,
    #[strum(serialize = "*")] Product,
    #[strum(serialize = "-")] Difference,
    #[strum(serialize = "/")] Quotient,
    #[strum(serialize = "floor/")] FloorDivide,
    #[strum(serialize = "truncate/")] TruncateDivide,
    #[strum(serialize = "floor")] Floor,
    #[strum(serialize = "ceiling")] Ceiling,
    #[strum(serialize = "truncate")] Truncate,
    #[strum(serialize = "round")] Round,
    #[strum(serialize = "exp")] Exp,
    #[strum(serialize = "log")] Log,
    #[strum(serialize = "sin")] Sine,
    #[strum(serialize = "cos")] Cosine,
    #[strum(serialize = "tan")] Tangent,
    #[strum(serialize = "asin")] Arcsine,
    #[strum(serialize = "acos")] Arccosine,
    #[strum(serialize = "atan")] Arctangent,
    #[strum(serialize = "sqrt")] SquareRoot,
    #[strum(serialize = "exact-integer-sqrt")] ExactSquareRoot,
    #[strum(serialize = "expt")] Exponent,
    #[strum(serialize = "inexact")] ToFloat,
    #[strum(serialize = "exact")] ToInt,

    #[strum(serialize = "not")] Not,
    #[strum(serialize = "boolean?")] IsBoolean_,
    #[strum(serialize = "boolean=?")] BooleanEq_,

    #[strum(serialize = "pair?")] IsPair_,
    #[strum(serialize = "cons")] Cons,
    #[strum(serialize = "car")] Car,
    #[strum(serialize = "cdr")] Cdr,
    #[strum(serialize = "set-car!")] SetCar,
    #[strum(serialize = "set-cdr!")] SetCdr,
    #[strum(serialize = "null?")] IsNull_,
    #[strum(serialize = "list?")] IsList_,
    #[strum(serialize = "make-list")] MakeList,
    #[strum(serialize = "list")] List,
    #[strum(serialize = "length")] Length,
    #[strum(serialize = "append")] Append,

    #[strum(serialize = "symbol?")] IsSymbol_,
    #[strum(serialize = "symbol=?")] SymbolEq_,
    #[strum(serialize = "symbol->string")] SymbolToString,
    #[strum(serialize = "string->symbol")] StringToSymbol,

    #[strum(serialize = "char?")] IsChar_,
    #[strum(serialize = "char=?")] CharEq_,
    #[strum(serialize = "char<?")] CharLt_,
    #[strum(serialize = "char->integer")] CharToInt,
    #[strum(serialize = "integer->char")] IntToChar,

    #[strum(serialize = "string?")] IsString_,
    #[strum(serialize = "make-string")] MakeString,
    #[strum(serialize = "string")] String,
    #[strum(serialize = "string-length")] StringLength,
    #[strum(serialize = "string-ref")] StringRef,
    #[strum(serialize = "string-set!")] StringSet,
    #[strum(serialize = "string=?")] StringEq_,
    #[strum(serialize = "string<?")] StringLt_,
    #[strum(serialize = "substring")] Substring,
    #[strum(serialize = "string-append")] StringAppend,
    #[strum(serialize = "string->list")] StringToList,
    #[strum(serialize = "list->string")] ListToString,
    #[strum(serialize = "string-copy")] StringCopy,
    #[strum(serialize = "string-copy!")] StringCopy_,
    #[strum(serialize = "string-fill!")] StringFill_,

    #[strum(serialize = "vector?")] IsVector_,
    #[strum(serialize = "make-vector")] MakeVector,
    #[strum(serialize = "vector")] Vector,
    #[strum(serialize = "vector-length")] VectorLength,
    #[strum(serialize = "vector-ref")] VectorRef,
    #[strum(serialize = "vector-set!")] VectorSet,
    #[strum(serialize = "vector->list")] VectorToList,
    #[strum(serialize = "list->vector")] ListToVector,
    #[strum(serialize = "vector-copy")] VectorCopy,
    #[strum(serialize = "vector-copy!")] VectorCopy_,
    #[strum(serialize = "vector-append")] VectorAppend,
    #[strum(serialize = "vector-fill!")] VectorFill_,

    #[strum(serialize = "bytevector?")] IsBytevector_,
    #[strum(serialize = "make-bytevector")] MakeBytevector,
    #[strum(serialize = "bytevector")] Bytevector,
    #[strum(serialize = "bytevector-length")] BytevectorLength,
    #[strum(serialize = "bytevector-u8-ref")] BytevectorU8Ref,
    #[strum(serialize = "bytevector-u8-set!")] BytevectorU8Set_,
    #[strum(serialize = "bytevector-copy")] BytevectorCopy,
    #[strum(serialize = "bytevector-copy!")] BytevectorCopy_,
    #[strum(serialize = "bytevector-append")] BytevectorAppend,

    #[strum(serialize = "procedure?")] IsProcedure_,
    #[strum(serialize = "apply")] Apply,
    #[strum(serialize = "call/cc")] CallWithCurrentContinuation,
    #[strum(serialize = "values")] Values,
    #[strum(serialize = "call-with-values")] CallWithValues,
    #[strum(serialize = "dynamic-wind")] DynamicWind,
    #[strum(serialize = "with-exception-handler")] WithExceptionHandler,
    #[strum(serialize = "raise")] Raise,
    #[strum(serialize = "raise-continuable")] RaiseContinuable,
    #[strum(serialize = "environment")] NewEvalEnvironment,
    #[strum(serialize = "eval")] Eval,

    // Internal (can only be introduced by compiler)
    #[strum(disabled)] StartInternalSentinel,
    Malloc,
    StoreDynamicExtent,
    RewindDynamicExtent,
    PushDynamicFrame,
    PushExceptionFrame,
    PushParameterFrame,
    FindExceptionHandler,
}

#[derive(Debug)]
pub struct Environment {
    pub macros: Vec<syntax::Rules>,
    pub toplevels: HashMap<Cow<'static, str>, Binding>,
    pub(super) bound_id_counter: std::cell::Cell<usize>,
    pub(super) arities: HashMap<ValueID, (Arity, Arity)>,
    pub(super) literals: HashMap<ValueID, std::rc::Rc<super::LiteralD>>,
}
impl Environment {
    pub fn new() -> Self {
        let (core_macro_names, core_macros) = syntax::core_macros();
        Self {
            macros: Vec::from(core_macros),
            toplevels: CoreForm::iter()
                .map(|cf| (Cow::Borrowed(cf.into()), Binding::CoreForm(cf)))
                .chain(CorePrimitive::iter().take((CorePrimitive::StartInternalSentinel as usize) - 1)
                    .map(|primitive| (Cow::Borrowed(primitive.into()), Binding::CorePrimitive(primitive))))
                .chain(core_macro_names.iter().copied().enumerate().map(|(i, s)| (Cow::Borrowed(s), Binding::SyntaxTransformer(i))))
                .collect(),
            bound_id_counter: std::cell::Cell::new(1),
            arities: Default::default(),
            literals: Default::default(),
        }
    }

    pub(super) fn new_id(&self) -> usize {
        let binding = self.bound_id_counter.get();
        self.bound_id_counter.set(binding.wrapping_add(1));
        binding
    }
}
