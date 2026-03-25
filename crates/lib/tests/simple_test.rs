#[test]
fn simple_pass() {
    // Given two integers whose sum is defined by arithmetic
    let left = 1;
    let right = 1;
    // When we add them
    let sum = left + right;
    // Then the result is two
    assert_eq!(sum, 2);
}
