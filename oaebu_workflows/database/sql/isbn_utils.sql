/*
 Copyright 2020 Curtin University

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 Author: Tuan Chien
*/

CREATE TEMP FUNCTION is_isbn13(input STRING)
RETURNS BOOLEAN
LANGUAGE js
AS r"""
function calc_isbn13_check_digit(isbn13) {
    var mask = [1,3,1,3,1,3,1,3,1,3,1,3];

    var prefix = [];
    for(let i = 0; i < 12; i++) {
        prefix.push(Number(isbn13[i]));
    }

    let check_digit = 0;
    for(let i = 0; i < 12; i++) {
        check_digit += mask[i]*prefix[i];
    }

    return (10-(check_digit % 10)) % 10;
}

if(input == null) {
    return false;
}

if(input.length != 13) {
    return false;
}

if(isNaN(Number(input))) {
    return false;
}

let check_digit = String(calc_isbn13_check_digit(input));
return check_digit == input[12];
""";


CREATE TEMP FUNCTION normalised_isbn(input STRING)
RETURNS STRING
LANGUAGE js
AS r"""

function calc_isbn13_check_digit(isbn13) {
    var mask = [1,3,1,3,1,3,1,3,1,3,1,3];

    var prefix = [];
    for(let i = 0; i < 12; i++) {
        prefix.push(Number(isbn13[i]));
    }

    let check_digit = 0;
    for(let i = 0; i < 12; i++) {
        check_digit += mask[i]*prefix[i];
    }

    return (10-(check_digit % 10)) % 10;
}

function calc_isbn10_check_digit(isbn10) {
    var mask = [10,9,8,7,6,5,4,3,2];

    var prefix = [];
    for(let i = 0; i < 9; i++) {
        prefix.push(Number(isbn10[i]));
    }

    let check_digit = 0;
    for(let i = 0; i < 9; i++) {
        check_digit += mask[i]*prefix[i];
    }

    check_digit = (11-(check_digit % 11)) % 11;

    if(check_digit == 10)
        return 'X';
    
    return check_digit;
}

function is_isbn13(isbn) {
    if(isbn.length != 13) {
        return false;
    }

    if(isNaN(Number(isbn))) {
        return false;
    }

    let check_digit = String(calc_isbn13_check_digit(isbn));
    return check_digit == isbn[12];
}

function is_isbn10(isbn) {
    if(isbn.length != 10) {
        return false;
    }

    if(isNaN(Number(isbn.slice(0,9)))) {
        return false;
    }

    let check_digit = String(calc_isbn10_check_digit(isbn));
    return check_digit == isbn[9];
}

function convert_isbn10_to_isbn13(isbn10) {
    let isbn = "978" + isbn10.slice(0, 9);
    let check_digit = calc_isbn13_check_digit(isbn);
    isbn += String(check_digit);
    return isbn;
}

function strip_isbn_string(isbn) {
    var regexp = /[^0-9X]/gi;
    return isbn.replace(regexp, "");
}

if(input == null) {
    return null;
}

// Check if valid ISBN10 or ISBN13
let stripped = strip_isbn_string(input);

if(stripped.length == 13 && is_isbn13(stripped)) {
    return stripped;
}

if(stripped.length == 10 && is_isbn10(stripped)) {
    return convert_isbn10_to_isbn13(stripped);
}

return null;

""";

