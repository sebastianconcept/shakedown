//! Fail test: #[shakedown::test] with wrong parameter signature
//! 
//! This should fail compilation because the first parameter must be ctx: &mut Context.

#[shakedown::test]
async fn wrong_param(ctx: shakedown::Context) {
    // Wrong - should be &mut Context, not owned value
}

fn main() {}
