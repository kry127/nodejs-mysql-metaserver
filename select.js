// select statement parser BNF notation:
/*
<select>::= SELECT <column list> FROM <table> {JOIN <table> ON <column>=<column> {AND <column>=<column>}};
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
    AND: "AND",
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

let lex_type = Object.freeze({
    OTHER: -1,
    KEYWORD: 0,
    VARIABLE: 1,
    OPERATOR: 2, // .,*;=
})

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
    function addLexem(type = lex_type.OTHER) {
        // check out lexem
        let lex = input.substr(i.position, j.position - i.position)
        // check if it is keyword
        if (keywords.indexOf(lex.toUpperCase()) != -1)
            type = lex_type.KEYWORD
        // add lexem (it can be variable or keyword)
        lex_arr.push({
            lexem: lex,
            type: type,
            column: i.column,
            row: i.row,
            position: i.position,
            end_position: j.position
        });
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
                addLexem(lex_type.OPERATOR);
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
                addLexem(lex_type.VARIABLE);
            }
            j.next()
            break;
        case 2:
            if (jchr == "`") {
                state = 0;
                addLexem(lex_type.VARIABLE);
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

// after lexer build it is time for abstract syntax tree
// here we check keyword ordering, correctness of table and column definition

// column raw definition
class node_column_raw {
    constructor(host, schema, table, column, alias) {
        this.host = host,
        this.schema =schema,
        this.table = table,
        this.column = column,
        this.alias = alias// for future :)
    }
}
// table proper definition
// host and schema are strings with no link to actual entities of the tree
// because they are not needed for further structure in AST
class node_table{
    constructor (host, schema, name, alias) {
        this.host = host, // simple string
        this.schema = schema, // simple string
        this.name = name,
        this.alias = alias // for future :)
    }
}
// column proper definition
class node_column {
    constructor(name, table, alias) {
        this.name = name,
        this.table = table, // link to node_table
        this.alias  = alias
    }
}
// SELECT statement node (raw and proper)
class node_select {
    constructor(table) {
        this.column_list = [], // array of node_column_raw  or node_column
        this.table = table, // link to node_table
        this.joins = [] // array of node_join or node_join_raw in refined style
    }
}
// ON statement node (raw and proper)
class node_on {
    constructor(left, right) {
        this.left = left, // left node_column_raw or node_column
        this.right = right // binds to right node_column_raw or node_column
    }
}
// JOIN statement node (raw and proper)
class node_join {
    constructor(table, ref_table=null) {
        this.table = table // link to node_table
        this.ref_table =ref_table // referenced table typeof node_table
        this.on = []// array of node_on or node_on_raw
        // this.on.left should link to this.table when used node_on
        // this.on.right should link to this.ref_table when used node_on
        // no other options are suggested by database
    }
}

// parsing strategy is simple:
//  - the root node is of SELECT type
//  - when finding column definition, add node_column_raw to the SELECT statement
//  - when finding FROM, add table property to select statement
//  - when finding JOIN, add node_join to the end of node_select
//  - in JOIN presented array of node_on, which presents pair of node_column[_raw]
// Then, on the builded tree, we should refine tree in semantic phase
// Semantics will depend on: currently using host.database, storing metadata

// input parameters:
//  1. sql source code
function ast(sql)
{
    var lexems = lexer(sql); // we incorporate sql parser in AST parser. Why not?

    class parse_error {
        constructor (msg, lex_id, end_lex_id) {
            this.msg = msg,
            this.lex_id = lex_id,
            this.end_lex_id = end_lex_id
        }
    }

    function throwError(i, msg) {
        let lex = lexems[i]
        var end_lex_id = i;
        while (end_lex_id < lexems.length - 1) {
            if (lexems[end_lex_id].lexem === ";")
                break
            end_lex_id++;
        }
        let end_lex = lexems[end_lex_id]
         // rest of SQL statement
        let rest_of_sql = lex ? sql.substr(lex.position, end_lex.end_position) : ""

        let err_mesg = `\
You have an SQL syntax error near '${lex?lex.lexem:"EOF"}'. \
Check NO manual, because this is garage crafted soft, that is no one needed in :) \
${(typeof msg !== "undefined" && msg) ? msg : ""} \
OurSQL: ${rest_of_sql}`
        
        throw new parse_error( err_mesg, i, end_lex_id); // can be catched for continuous parsing
    }

    function next(i) {
        i++
        if (i >= lexems.length)
            throwError("Unexpected EOF")
        return i
    }

    // define some easy parsers
    // ALL parsers return structure:
    // {node: <result of parsing>, index: <next index to look>}
    
    // BNF: <general column>, <column>
    function parseColumn(i) // obviously, parses columns
    {
        var tokens = []; // up to 4 tokens
        // parsing stops when one of the following
        //  - star (*) occured -- it should be last lexem
        //  - four tokens found
        // second token can be empty (supplied host without supplied schema)
        var empty_schema = false;
        k_cycle: for (let k = 0; k < 4; k++) {
            switch (lexems[i].type){
            case lex_type.VARIABLE:
                tokens.push(lexems[i].lexem)
                break;
            case lex_type.OPERATOR:
                if (lexems[i].lexem === '*') {
                    if (!empty_schema || k == 3) {
                        tokens.push("*")
                        i = next(i)
                        break k_cycle // end of parsing
                    } else
                        throwError(i, "The star allowed only at last position, even when no schema provided.")
                }
                else if (lexems[i].lexem === '.') {
                    if (k == 1) { // empty schema is allowed
                        empty_schema = true // but instead we expect fully qualified name (no star at table name)
                        tokens.push(null)
                    } else
                        throwError(i, "Only empty schema is allowed (in context of USE [HOST].[SCHEMA]")
                } else {
                    // throw an error
                    throwError(i, "Unexpected operator in column specificator.")
                }
                break;
            default:
                throwError(i, "Unexpected token in column specificator.")
                break;
            }
            // then, move to next token
            // if it is operator or keyword, end parsing
            i = next(i)
            if (lexems[i].lexem == ".") {
                if (k < 3)
                    i = next(i) // moving to analyze next lexem
                else throwError(i, "Unexpected token: five level qualifier for column")
            } else if (lexems[i].type === lex_type.KEYWORD || lexems[i].type === lex_type.OPERATOR) {
                if (empty_schema && k < 3) {
                    throwError(i, `Expected, that schema is not supplied, but instead there is no table supplied in column identifier.`)
                }
                break k_cycle
            } else throwError(i, "Unexpected token, instead keyword or dot expected")
        }

        // making return
        var node = new node_column_raw()
        node.column = tokens.pop() || null
        node.table = tokens.pop() || null
        node.schema = tokens.pop() || null
        node.host = tokens.pop() || null
        return {
            node: node,
            index: i
        }
    }

    // BNF: <table>
    function parseTable(i) {
        var tokens = []; // up to 3 tokens
        // second token can be empty (supplied host without supplied schema)
        
        var empty_schema = false
        k_cycle: for (let k = 0; k < 3; k++) {
            switch (lexems[i].type){
            case lex_type.VARIABLE:
                tokens.push(lexems[i].lexem)
                break;
            case lex_type.OPERATOR:
                if (lexems[i] === '.') {
                    if (k == 1) { // empty schema is allowed
                        empty_schema = true // but instead we expect fully qualified name
                        tokens.push(null)
                    } else
                        throwError(i, "Only empty schema is allowed (in context of USE [HOST].[SCHEMA]")
                } else {
                    // throw an error
                    throwError(i, "Unexpected operator in table specificator.")
                }
                break;
            default:
                throwError(i, "Unexpected token in table specificator.")
                break;
            }
            // then, move to next token
            // if it is operator or keyword, end parsing
            i = next(i)
            if (lexems[i].lexem == ".") {
                if (k < 2)
                    i = next(i) // moving to analyze next lexem
                else throwError(i, "Unexpected token: four level qualifier for table")
            } else if (lexems[i].type === lex_type.KEYWORD || lexems[i].type === lex_type.OPERATOR) {
                if (empty_schema && k < 2) {
                    throwError(i, `Expected, that schema is not supplied, but instead there is no table supplied in column identifier.`)
                }
                break k_cycle
            } else throwError(i, "Unexpected token, instead keyword or dot expected")
        }
        
        // making return
        var node = new node_table()
        node.name = tokens.pop() || null
        node.schema = tokens.pop() || null
        node.host = tokens.pop() || null
        return {
            node: node,
            index: i
        }
    }

    // BNF: <column list>
    function parseColumnList(i) {
        var ret =[]
        do {
            var result = parseColumn(i)
            ret.push(result.node)
            i = result.index
            if (lexems[i].type != lex_type.OPERATOR || lexems[i].lexem !== ",")
                break;
            i = next(i)
        }
        while (true)
        return {
            node: ret,
            index: i
        }
    }

    // BNF: <column>=<column>
    // follows after ON and AND in JOIN statement
    function parseOn(i) {
        var result = parseColumn(i) // parse first column
        var left = result.node
        if (left.column === "*")
            throwError(i, "Not allowed to use generalized column in 'ON' statement")
        i = result.index
        // check binding
        if (lexems[i].type !== lex_type.OPERATOR || lexems[i].lexem !== "=")
            throwError(i, "Expected equality binding of columns")
        i = next(i) // parse second column
        result = parseColumn(i)
        var right = result.node
        if (right.column === "*")
            throwError(i, "Not allowed to use generalized column in 'ON' statement")
        return {
            node: new node_on(left, right),
            index: result.index
        }
    }

    // BNF: <table> ON <column>=<column> {AND <column>=<column>}
    // stops at the end when any keyword found except 'AND' keyword
    function parseJoin(i) {
        var table = parseTable(i)
        i = table.index
        var node = new node_join(table.node)
        // check if "ON" keyword presented
        if (lexems[i].type != lex_type.KEYWORD || lexems[i].lexem.toUpperCase() != keywords.ON)
            throwError(i, "'ON' keyword expected")
        i = next(i) // next lexem
        // parse ON statement
        do {
            var on = parseOn(i)
            node.on.push(on.node)
            i = on.index
            if (lexems[i].lexem.toUpperCase() !== keywords.AND)
                break
            i = next(i)
        } while (true)
        // forming return
        return {
            node: node,
            index: i
        }
    }

    // BNF: <column list> FROM <table> {JOIN <table> ON <column>=<column> {AND <column>=<column>}}
    // Stops when ';' found
    function parseSelect(i) {
        var node_column_list = parseColumnList(i)
        i = node_column_list.index
        if (lexems[i].lexem.toUpperCase() !== keywords.FROM)
            throwError(i, "'FROM' Keyword expected")
        var node_table = parseTable(next(i))
        i = node_table.index
        var node = new node_select(node_table.node) // primary node
        node.column_list = node_column_list.node

        // parsing JOINS if presented
        while(lexems[i].lexem.toUpperCase() === keywords.JOIN) {
            var node_join = parseJoin(next(i))
            i = node_join.index
            node.joins.push(node_join.node)
        }

        // check ;
        if (lexems[i].lexem != ";")
            throwError(i, "Expected end of SELECT statement")

        if (i < lexems.length - 1) // stay either on last ";", or on next statement
            i = next(i);
        return {
            node: node,
            index: i
        }
    }

    // BNF: {<select>;}
    // we have now only select sequence :)
    function parseSequence(i) {
        var seq = []
        while (i < lexems.length) {
            try {
                if (lexems[i].lexem.toUpperCase() === keywords.SELECT) {
                    var node_select = parseSelect(next(i));
                    seq.push(node_select.node)
                    i = node_select.index
                } else if (lexems[i].lexem === ";"){
                    if (i < lexems.length - 1)
                        i = next(i) // nop
                    else break; // end of recognition
                } else {
                    throwError(i, "Keyword expected")
                }
            } catch (error) {
                if (error instanceof parse_error) {
                    // catch only parsing errors
                    console.error(error.msg)
                    i = error.end_lex_id + 1
                } else throw error
            }
        }
        return seq
    }

    return parseSequence(0)
}

// Semantic analysis
//  1. for table in FROM statement:
//   1.1. check existence
//     it means in AST presented context host.database.table and table record presented
//   1.2. get all columns of table, store them in node if needed
//  2. for every JOIN statement:
//   2.1. do steps 1.1 and 1.2 for table in JOIN statement
//   2.2. for statement ON of JOIN:
//    2.2.1. check, if left column belongs to JOIN statement
//           if not, get array of tables that column can belong to (from upper tables)
//    2.2.2. check, if right column belongs to JOIN statement
//           if not, get array of tables that column can belong to (from upper tables)
//    2.2.3. if array is empty, giveout appropriate message
//           if array contains more than one table, inform user that he should disambiguate column
//    2.2.4. make column belonging to JOIN statement table left.
//           if no such column presented, error should be rised
//    2.2.5. place ref_table attribute of the JOIN statement to the value of table,
//           that refers right column
//   2.3. for statement AND of JOIN:
//    2.3.1 do steps 2.2
//    2.3.2 check that right binding table stays the same (equals ref_table)
//          otherwize error should be rised
//   2.4. restructure JOIN node: convert node_column_raw to node_column (if needed)
//   2.5. check, that defined foreign key exists in metadata server
//  3. for columns of SELECT statement check presense of all listed columns in tables of statements
//     FROM and JOIN.
//     Exception: column '*' with no context.
//     If column definition is orphan, then should be given apropriate message
//     If two or more tables can contain compatible column definition, message should be provided
//     for table disambiguation. (note, that two tables with the same name cannot be 
//     disambiguated without aliases)

// the question is: how to combine callback pattern with semantic parser pattern ???


// tests
var sampleSQL = "SELECT STUDENT.*,STUDENT.ID \nFROM STUDENT \nwhere `lol`=`kek cheburek`"
var parsedSQL = ast(sampleSQL);

var sql1 = "SELECT * FROM USERS;"
var sql2 = "SELECT A.a, B.b FROM A JOIN M ON M.a = A.a AND M.b = B.b JOIN K ON K.m = M.m;"
var sql3 = "SELECT A FROM B; SELECT B FROM C; select C from `a s d f g ; e $`;"

var psql1 = ast(sql1);
var psql2 = ast(sql2);
var psql3 = ast(sql3);

var nop =  0;