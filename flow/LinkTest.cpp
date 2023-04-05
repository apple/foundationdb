// When creating a static or shared library, undefined symbols will be ignored.
// Since we want to ensure no symbols from other modules are used, each module
// will create an executable so the linker will throw errors if it can't find
// the declaration of a symbol. This class defines a dummy main function so the
// executable can be built.
int main() {
	return 0;
}
