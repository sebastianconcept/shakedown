# shakedown-macros

Procedural macros for Shakedown testing framework.

## Status: Stub

This crate provides procedural macros (`#[system_test]`, `#[faulty_test]`, etc.) that expand to calls into the **`shakedown`** library API.

### Current status

- **Milestone**: `mvp-foundation`
- **Status**: Stub
- **Purpose**: Workspace layout and linkage smoke test

### What's implemented

This crate currently contains only the basic structure:
- Package name: `shakedown-macros`
- Proc-macro library setup
- Dependencies (_quote_, etc.)

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
