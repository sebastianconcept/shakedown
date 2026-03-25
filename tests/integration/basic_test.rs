use shakedown::Context;

#[shakedown::test]
async fn simple_integration_test(ctx: &mut Context) {
    assert!(true);
}

#[test]
fn regular_test() {
    assert!(true);
}
