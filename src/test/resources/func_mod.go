// copy of the file from function duplicates fixture for query test
// function A changed (returns false instead of true)
// function B is the same
package main

// A is just a function
func A() bool {
	return false
}

// B contains another function inside
func B() {
	c := func() bool {
		return false
	}
	c()
}
