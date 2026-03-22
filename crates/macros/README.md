# shakedown-macros

Procedural macros for Shakedown testing framework.

## Status: Foundation - register_resource! implemented

This crate provides procedural macros for the Shakedown testing framework.

### Current status

- **Milestone**: `mvp-foundation`
- **Status**: Implemented
- **Purpose**: Core procedural macros for resource registration

### What's implemented

This crate currently implements:

- **`register_resource!`** - Declarative resource registration macro
  - Attribute macro for marking custom resources
  - Generates boilerplate for Shakedown resource trait implementation

- **`default_greeting!`** - Compile-time greeting string macro (for tests)

### What's next

Full macro implementations (`#[system_test]`, `#[faulty_test]`, etc.) will land in **`mvp-macros-system-test`** (post-`mvp-foundation`). The API design is documented in:

- [Product PRD](../../feature/mvp-foundation/PRD.md)
- [Technical SPECS](../../feature/mvp-foundation/SPECS.md)
- [Open Questions](../../feature/mvp-foundation/OPEN-QUESTIONS.md)

### Integration

When fully implemented, macros in this crate will:

1. Parse attribute syntax (`#[system_test]`, `#[faulty_test]`)
2. Expand to Rust code that calls **`shakedown`** library APIs
3. Generate test harness boilerplate (context setup, lifecycle orchestration)

### Dependencies

- **`shakedown`**: The library crate containing all orchestration logic (lifecycle, resource providers, error types)

### Workspace layout

```
shakedown/
├── crates/
│   ├── lib/          # shakedown (orchestration engine)
│   ├── macros/       # this crate: procedural macros
│   └── cli/          # shakedown-cli (thin caller over lib)
```

### See also

- [Shakedown README](../README.md) — project overview
- [TECH-STACK.md](../../TECH-STACK.md) — dependencies and tooling
- [ARCHITECTURE.md](../../ARCHITECTURE.md) — design decisions
