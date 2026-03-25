//! Pass test: #[shakedown::test] with resources argument
//! 
//! This verifies that the resources arg works correctly for auto-registering Fake.

#[shakedown_macros::test(resources = [Fake])]
async fn test_with_fake(ctx: &mut shakedown::Context) {
    assert!(!ctx.is_empty());
}

fn main() {}
