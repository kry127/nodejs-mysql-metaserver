// select statement parser BNF notation:
/*
<select>::= SELECT <column list> FROM <table> {JOIN <table> ON <column>=<column>};
<column list> ::= <general column> {, <general column>}
<general column> ::= <column> | <table>.* | *
<column> ::= [<table>.]<name>
<table> ::= [<schema>.]<name>
<schema> ::= [<host>.]<name> | <host>.
<host> ::= <name>
<name> ::= <alpha>{<alnum>} | `<alpha>{<alnum>|<space>}`
<alnum> ::= <alpha> | <number>
<alpha> ::= A|B|C|D|E|F|G|H|I|J|K|L|M|N|O|P|Q|R|S|T|U|V|W|X|Y|Z|a|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|v|w|x|y|z
<number> ::= 0|1|2|3|4|5|6|7|8|9
<space> ::=  |\t|\n|\v|\r
*/

// keywords object
let keywords = Object.freeze({
    SELECT: "SELECT",
    FROM: "FROM",
    JOIN: "JOIN",
    ON: "ON",
    indexOf: function(str) {
        var i = 0;
        for (var key in this) {
            if (str === this[key])
                return i
            else i++
        }
        return -1;
    }
});

// lexer
function lexer(input) {
    // begining and ending of analyzing lexem
    // now it shoud store information not even about lexems, but about position in text :)
    var i = {
        position:0,
        column:0,
        row:0,
        next: function() {this.position++;this.column++},
        newline: function() {this.position++;this.column=0;this.row++},
        clone: function() { return Object.assign({}, this) }
    }
    var j = i.clone(); // deeply clones, even function :)
    var lex_arr = []; // {lexem:"", column:0, row:0}
    // addition of lexem
    function addLexem() {
        // check out lexem
        let lex = input.substr(i.position, j.position - i.position)
        // add lexem (it can be variable or keyword)
        lex_arr.push({lexem: lex, column: i.column, row: i.row});
        // refresh index
        i = j.clone();
    }
    
    function alpha(char) {char = char.toLowerCase(); return char >= 'a' && char <= 'z'}
    function number(char) {return char >= '0' && char <= '9'}
    function space(char) {return " \n\t\v\r".indexOf(char) != -1}

    // let's make it like state machine
    var state = 0
    while (i.position < input.length && j.position <= input.length) {
        let ichr = input.charAt(i.position).toLowerCase()
        let jchr = input.charAt(j.position).toLowerCase()
        switch(state) {
        case 0: // initial state
            if (alpha(ichr)) { // <name> or keyword
                j = i.clone()
                state = 1;
            } else if (ichr == '`') { // <name>, but with spaces
                i.next() // lookin at next symbol immediately
                j = i.clone()
                state = 2;
            } else if (".,=;*".indexOf(ichr) != -1) { // single lexems
                j = i.clone();
                j.next();
                addLexem();
            } else if (ichr == '\n') {
                i.newline(); // register newline
            } else if (space(ichr)) {
                i.next(); // skip unnessessary spaces
            } else {
                state = 1000; // error state
            }
            break;
        case 1:
            if (!alpha(jchr) && !number(jchr)) {
                state = 0
                addLexem();
            }
            j.next()
            break;
        case 2:
            if (jchr == "`") {
                state = 0;
                addLexem();
                i.next(); // automatic next i, because no need in `
            }
            j.next()
            break;
        case 1000: // error state
            let token = input.substr(i.position, j.position - i.position)
            throw `Unexpected token: ${token} at column ${i.column} line ${i.row}!`;
        }
    }
    if (state != 0) {
        let token = input.substr(i.position, j.position - i.position)
        throw `Unexpected end of file: ${token} at column ${i.column} line ${i.row}!`
    }
    return lex_arr;
}

var sampleSQL = "SELECT *,ID \nFROM STUDENTS \nwhere `lol`=`kek cheburek`"
var parsedSQL = lexer(sampleSQL);

var nop =  0;