# Styleguide
## Header Files

### File Names
* Header Files `.hpp`
* Source Files `.cpp`
* All lowercase 

### The #define guard
* `#pragma once`

### Forward Declarations
* You may forward declare ordinary classes in order to avoid unnecessary `#includes`

### Inline Functions
* No Restrictions on number of lines
* Either inside or outside classes

### Function Parameter Ordering 
* First inputs, then outputs

### Names and Order of Includes
* First include should be corresponding header file

## Scoping
### Namespaces
* Don't use `namespace std`
* Declare a project namespace
* Unnamed Namespaces are a possibility
* You can use the project namespace in applications

### Nested Classes
* Don't use public nested classes

### Local Variables
* Place a function's variables in the narrowest scope possible
* Initialize variables in the declaration

### Classes 
* Avoid doing complex initialization in constructors
* If your object requires non-trivial initialization, use an `init` function

### Initialization
* Initialize everything!

### Explicit Constructor
* Use the C++ keyword explicit for constructors callable with one argument

### Copyable and Movable Types
* Disable `copy` and `move` when possible

### Delegating and Inheriting Constructors
* Use delegating and inheriting constructors

### Structs and Classes 
* Use a struct only for passive objects that carry data
* Everything else is a class

### Inheritance 
* Composition is often more appropriate than inheritance
* When using inheritance, make it public
* Only very rarely is multiple inheritance actually useful! (Don't use it)

### Interface
* Only call something Interface if it is purely virtual

### Operator Overloading
* Don't use operator overloading!

### Declaration Order
* `public`, `protected`, `private`
* Typedefs and Enums
* Constants (static const data members)
* Constructors
* Destructor
* Methods, including static methods
* Data Members (except static const data members)

### Write Short Functions
* Everything over 40 lines has to do something useful!

## C++ Features
### Ownership and Smart Pointers
* Avoid using raw-pointers and references
* Use smart pointers!

### Reference Arguments
* All parameters passed by reference must be labeled `const`

### Default Arguments
* Use default arguments
* Don't use function overloading

### Exceptions
* Don't use exceptions

### Casting
* Use C++ style casts (dynamic, static) when needed
* Don't use C style casts

### Streams
* Use streams for logging
* Streams can be used for parsing input arguments

### Use of `const`
* `const` all the things!
* Put it in front of declarations

### Integer Types
* Use types from `<stdint.h>` or `size_t`
* Don't use `int, long, ...`!

### `NULL/nullptr`
* Use `nullptr` for pointers

### `auto`
* Use `auto` against clutter
* Don't use `auto` for trivial types

### Template metaprogramming
* Template programming is allowed

## Naming
### Files Names
* All lowercase 
* Underscores for separation

### Type Names (Classes, Typedefs, etc.)
* Camelcase
* Start with captial

### Variable Names
* All lowercase 
* Underscores for separation

### Class Data Members
* Trailing underscores

### Constant Names
* `k` followed by mixed case

### Function Names
* Regular functions camelcase. Start with captial letters
* Accessors/mutators lowercase + underscores

### Enumerators
* Treat like constants

## Comments 
### General
* Use Doxygen! 

### Class Comments 
* Leave aside

### Implementation Comments 
* Describe function parameters if they are non trivial
* Constants are a possibility for self-explanatory code

### TODO Comments 
* Additionaly use Github/JIRA features 

### Formatting 
### Line length 
* 80 characters

### Tabs vs. Spaces
* Only spaces 
* 4 spaces

### Conditionals 
* Short if-statements in a single line 

### Variable and Array Initialization 
* Initialize with `()`

### Class Format 
* `private, public, protected` on same line as class 
 

