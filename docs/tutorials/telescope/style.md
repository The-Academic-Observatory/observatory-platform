# Style
All code should try to conform to the Python PEP-8 standard, and the default format style of the `Black` formatter.
This is done with the [autopep8 package](https://pypi.org/project/autopep8), and the 
 [black formatter](https://pypi.org/project/black/), using a line length of 120.

It is recommended to use those format tools as part of the coding workflow.

## Type hinting
Type hints should be provided for all of the function arguments that are used, and for return types. 
Because Python is a weakly typed language, it can be confusing to those unacquainted with the codebase what type of 
 objects are being manipulated in a particular function.
Type hints help reduce this ambiguity.

## Docstring
Docstring comments should also be provided for all classes, methods, and functions. 
This includes descriptions of arguments, and returned objects.  
These comments will be automatically compiled into the Observatory Platform API reference documentation section.