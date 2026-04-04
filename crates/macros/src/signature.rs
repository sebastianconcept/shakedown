//! Shared signature checks for scenario test attribute macros.

use syn::{FnArg, ItemFn, Type};

/// True when `ty` is `&mut ...::Context` (mutable reference to a path whose last segment is `Context`).
pub(crate) fn is_ctx_mut_context_type(ty: &Type) -> bool {
    let Type::Reference(type_reference) = ty else {
        return false;
    };
    if type_reference.mutability.is_none() {
        return false;
    }
    let Type::Path(type_path) = type_reference.elem.as_ref() else {
        return false;
    };
    type_path
        .path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "Context")
}

/// Returns `true` when the first parameter is `ctx: &mut …::Context`.
pub(crate) fn first_parameter_is_ctx_mut_context(func: &ItemFn) -> bool {
    let Some(first) = func.sig.inputs.first() else {
        return false;
    };
    let FnArg::Typed(pat_type) = first else {
        return false;
    };
    is_ctx_mut_context_type(pat_type.ty.as_ref())
}
