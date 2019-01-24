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
<alnum> ::= <alpha> | <digit>
<alpha> ::= _|A|B|C|D|E|F|G|H|I|J|K|L|M|N|O|P|Q|R|S|T|U|V|W|X|Y|Z|a|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|v|w|x|y|z
<digit> ::= 0|1|2|3|4|5|6|7|8|9
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
    VARIABLE_OR_KEYWORD: 17
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
        if (keywords.indexOf(lex.toUpperCase()) != -1 && type == lex_type.VARIABLE_OR_KEYWORD)
            type = lex_type.KEYWORD
        if  (type == lex_type.VARIABLE_OR_KEYWORD)
            type = lex_type.VARIABLE
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
    
    function alpha(char) {char = char.toLowerCase(); return char >= 'a' && char <= 'z' || char == "_"}
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
            } else if (".,=;*-".indexOf(ichr) != -1) { // single lexems
                if (input.substr(i.position, 2) === "--") {
                    // this is comment
                    state = 3;
                    break;
                }
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
                addLexem(lex_type.VARIABLE_OR_KEYWORD);
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
        case 3:
            if (ichr == "\n") {
                state = 0;
            }
            i.next();
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
    toString() {
        if (this.host && !this.schema)
            return [this.host, " ", this.table, this.column].filter(e=>e).join(".")
        else
            return [this.host, this.schema, this.table, this.column].filter(e=>e).join(".")
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
    toString() {
        return [this.host, this.schema, this.name].filter(e=>e).join(".")
    }
}
// column proper definition
class node_column {
    constructor(name, table, alias) {
        this.name = name,
        this.table = table, // link to node_table
        this.alias  = alias
    }
    toString() {
        return this.table.toString() + "." + this.name
    }
}
// SELECT statement node (raw and proper)
class node_select {
    constructor(table) {
        this.column_list = [], // array of node_column_raw  or node_column
        this.table = table, // link to node_table
        this.joins = [] // array of node_join or node_join_raw in refined style
        this.sql = null // sql statement of SELECT
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

// also define class parse_error:
class parse_error {
    constructor (msg, lexem_from, lexem_end) {
        this.msg = msg,
        this.lexem_from = lexem_from,
        this.lexem_end = lexem_end
    }
    toString() {
        return this.msg;
    }
}

// parsing strategy is simple:
//  - the root node is of SELECT type
//  - when finding column definition, add node_column_raw to the SELECT statement
//  - when finding FROM, add table property to select statement
//  - when finding JOIN, add node_join to the end of node_select
//  - in JOIN presented array of node_on, which presents pair of node_column[_raw]
// Then, on the builded tree, we should refine tree in semantic phase
// Semantics will depend on: currently using host.schema, storing metadata

// input parameters:
//  1. sql source code
function ast(sql)
{
    var lexems = lexer(sql); // we incorporate sql parser in AST parser. Why not?

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
        let rest_of_sql = lex ? sql.substr(lex.position, end_lex.end_position - lex.position) : ""

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
            throwError(i-1, "Unexpected EOF")
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
                        i--; // firing "next" on i is the next step
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
                if (lexems[i].lexem === '.') {
                    if (k == 1) { // empty schema is allowed
                        empty_schema = true // but instead we expect fully qualified name
                        i--; // firing "next" on i is the next step
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

        i++ // WARNING: can point beyond lexems, this is intentional
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
            var j = i
            try {
                if (lexems[i].lexem.toUpperCase() === keywords.SELECT) {
                    var node_select = parseSelect(next(i));
                    i = node_select.index
                    // copy SQL statement to node_select
                    node_select.node.sql = {
                        lexem_begin: j,
                        lexem_end: i-1,
                        lexems: lexems.slice(j, i),
                        str: sql.substr(lexems[j].position, lexems[i-1].end_position - lexems[j].position)
                    }
                    seq.push(node_select.node)
                    
                } else if (lexems[i].lexem === ";"){
                    i++ // nop
                } else {
                    throwError(i, "Keyword expected")
                }
            } catch (error) {
                if (error instanceof parse_error) {
                    error.sql = {
                        lexem_begin: j,
                        lexem_end: error.lexem_end,
                        lexems: lexems.slice(j,error.lexem_end+1),
                        str: sql.substr(lexems[j].position, lexems[error.lexem_end].end_position-lexems[j].position),
                    }
                    // catch only parsing errors
                    seq.push(error) // let user know about errors
                    i = error.lexem_end + 1
                } else throw error
            }
        }
        return seq
    }

    return parseSequence(0)
}

// Semantic analysis
//
//  1. for table in FROM statement:
//-> 1.1. check existence
//     it means in AST presented context host.schema.table and table record presented
//-> 1.2. get all columns of table, store them in node if needed
//  2. for every JOIN statement:
//-> 2.1. do steps 1.1 and 1.2 for table in JOIN statement
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
//   2.3. for each statement AND of JOIN:
//    2.3.1 do steps 2.2
//    2.3.2 check that right binding table stays the same (equals ref_table)
//          otherwize error should be rised
//   2.4. restructure JOIN node: convert node_column_raw to node_column (if needed)
//-> 2.5. check, that defined foreign key exists in metadata server
//  3. for columns of SELECT statement check presense of all listed columns in tables of statements
//     FROM and JOIN.
//     Exception: column '*' with no context.
//     If column definition is orphan, then should be given apropriate message
//     If two or more tables can contain compatible column definition, message should be provided
//     for table disambiguation. (note, that two tables with the same name cannot be 
//     disambiguated without aliases)

// the question is: how to combine callback pattern with semantic parser pattern ???

// make 3 independent parser phases, that implements algorithm:
// 1. check existence of all presented tables and retrieve their columns (BFS): 1, 2.1
// 2. make all semantic analysis: 2.2, 2.3, 2.4, 3
// 3. check foreign key existence: 2.5
// pitifully, there should be callback chaining, because steps 1 and 3 depends on async MySQL
// so 1 and 3 would be nonblocking and should accept callback result


// metaserver connection for semantic parser
var metasrv = require('./metaserver');


function semantic(sql, callback, environment) {
    // through environ we will look at defined host and schema
    // maybe store some other syncing information, like state machine
    var env = environment; // shorthand of this long name
    if (typeof environment === "undefined") {
        env = {} // .host and .schema could be defined here
    }

    // let's incorporate AST builder in semantic builder
    env.ast_array = ast(sql);
    env.ast_array_i = 0;
    // ast_array contains node_select and parse_error
    // both of them contain object "sql", which has the original SQL, lexems of the query
    // so semantic parser has access to source code
    
    env.phase = 1; // begin with first phase
    env.state = 0; // state -- initial
    phase1(); // start

    function throwError(object) {
        if (typeof callback === "function")
            setTimeout(function() { // we should make it truly async
                if (object instanceof parse_error) {
                    callback(object)
                    return
                }
                callback( new parse_error(object, 0, 0))
            }, 0);
            
        nextAST()
        setTimeout(function() {
            phase1() // call phase1 again with next asts
        }, 0);
    }

    function nextAST() {
        env.ast_array_i++;
        env.phase = 1;
        env.state = 0;
    }

    // checks that column can be derived from table
    function columnCompliantToTable(_node_table, _node_column_raw) {
        if (!_node_table.columns
            || _node_column_raw.column!=="*"
            && _node_table.columns.indexOf(_node_column_raw.column) == -1)
            return false; // principally cannot be true
        if (_node_column_raw.table && _node_column_raw.table != _node_table.name)
            return false; // specified table  in column definition differs
        if (_node_column_raw.schema && _node_column_raw.schema != _node_table.schema)
            return false; // specified schema in column definition differs
        if (_node_column_raw.host && _node_column_raw.host != _node_table.host)
            return false; // specified schema in column definition differs
        return true;
    }

    // parser phase 1 -- table processing
    function phase1(err, result) {
        if (err) {
            if (typeof callback === "function")
                callback(err, null); //an error occured, should be processed in upstream
            return;
        }

        switch_repeat: do {
            switch (env.state) {
            case 0:
                // get next AST result, if any
                if (env.ast_array_i >= env.ast_array.length) {
                    // end, if no AST result presented
                    env.state=3250;
                    metasrv.disconnect(phase1); // close connection to metasql server
                    return;
                }

                var ast = env.ast_array[env.ast_array_i]
                // check if it is parse error, if it is, give appropriate callback to user
                if (ast instanceof parse_error) {
                    return throwError(ast); // restart phase 1 with next ast
                } else if (!(ast instanceof node_select)) {
                    // assume we processing only node_selects
                    return throwError("Internal parsing error: root node is not of type 'node_select'.");
                }
                // here guaranteed, that ast will be valid
                // get all tables in tree structure, gather them into single array
                env.tables = [ast.table]
                env.tables.push(...ast.joins.map(join=>join.table))
                env.tables_i = 0
                env.state++ // move to next state
                metasrv.connect(phase1); // connect to metasql server
                return; // wait for callback
            case 1:
                // get table if any
                if (env.tables_i >= env.tables.length)
                    return; // end, if no AST result presented
                var table = env.tables[env.tables_i]
                // check if it is typeof node_table
                if (!(table instanceof node_table))
                {
                    return throwError("Internal parsing error: exptected 'node_table'.");
                }
                // supply table with host and schema from environment, if needed
                if (!table.host)  {
                    if (env.host)
                        table.host = env.host
                    else 
                        return throwError(`Host is not specified for table ${table}`);
                }
                if (!table.schema)  {
                    if (env.schema)
                        table.schema = env.schema
                    else 
                        return throwError(`Schema is not specified for table ${table}`);
                }
                env.state++ // next state -- check table existence
                metasrv.checkTable({host: table.host, schema: table.schema, table: table.name}, phase1)
                return; // wait for callback
            case 2:
                var table = env.tables[env.tables_i]
                if (!result) {
                    return throwError(`Table ${table} is not exist`);
                }
                // now let's get columns of table
                env.state++ // next state
                metasrv.getTableColumns({host: table.host, schema: table.schema, table: table.name}, phase1);
                return; //wait for callback
            case 3:
                var table = env.tables[env.tables_i]
                table.columns = result // save list of columns :)
                // check, if all tables were considered
                if (env.tables_i < env.tables.length - 1) {
                    env.tables_i++
                    env.state = 1
                    break // continue with state 1 -- check table
                }
                // all tables considered, moving to phase 2
                env.state++;
                break;
            case 4:
                // here we should map columns to tables
                // joins
                var ast = env.ast_array[env.ast_array_i]
                var list_tbl = [ast.table] // for "looking up" for columns
                for (let j = 0; j < ast.joins.length; j++) {
                    var join = ast.joins[j]
                    for (let o = 0; o < join.on.length; o++) {
                        var on = join.on[o];
                        // check, who is compliant to join's table
                        let l_comp = columnCompliantToTable(join.table, on.left)
                        let r_comp = columnCompliantToTable(join.table, on.right)
                        // not necessary condition really, but why not?
                        if ((l_comp || r_comp) == false)
                            return throwError(`ON statement for table ${join.table} is invalid: both columns ${on.left} and ${on.right} are not belong to the table.`)
                        // let's define, that (but not really necessary too)
                        if (l_comp && r_comp)
                            return throwError(`Table ${join.table} shouldn't bind to itself by ${on.left} = ${on.right} condition.`)
                        // swap tables, so the column that refers join's table would be left
                        if (r_comp && !l_comp) {
                            [on.left, on.right] = [on.right, on.left]
                        }
                        // find, what table refers on.right
                        var r_table;
                        if (!(l_comp && r_comp)) {
                            var list_tbl_comp = list_tbl.filter(t=>columnCompliantToTable(t, on.right))
                            if (list_tbl_comp.length == 0) {
                                return throwError(`No appropriate table for column ${on.right.column}`)
                            } else if (list_tbl_comp.length > 1) {
                                return throwError(`The column ${on.right.column} is ambigulous`)
                            }
                            r_table = list_tbl_comp[0];
                        }
                        // restructure both nodes
                        var l_node = new node_column(on.left.column, join.table, on.left.alias);
                        var r_node = new node_column(on.right.column, r_table, on.right.alias);
                        // restructure on node
                        [on.left, on.right] = [l_node, r_node]
                    }
                    list_tbl.push(join.table)
                }
                // check, that list_tbl contains defined columns
                for (let c = 0; c < ast.column_list.length; c++) {
                    let col = ast.column_list[c];

                    var list_tbl_comp = list_tbl.filter(t=>columnCompliantToTable(t, col))
                    if (list_tbl_comp.length == 0) {
                        return throwError(`No appropriate table for column ${col}`)
                    } else if (list_tbl_comp.length > 1 && col.column !== "*") {
                        return throwError(`The column ${col} is ambigulous`)
                    }
                    var table = list_tbl_comp[0];
                    // restructure column node
                    var col_node = new node_column(col.column, r_table, col.alias);
                    // restructure select node
                    ast.column_list[c] = col_node
                }
                if (typeof callback === "function")
                    setTimeout(function() { // we should make it truly async
                        callback(null, ast)
                    }, 0);
                nextAST();

            case 3250: //  for closing connection
                break;
            }
        } while (true);
    } 
}

module.exports = {
    // export only semantic analyzer
    semantic: semantic
}


// tests
/*var sampleSQL = "SELECT STUDENT.*,STUDENT.ID \nFROM STUDENT \nwhere `lol`=`kek cheburek`"
var parsedSQL = ast(sampleSQL);

var sql1 = "SELECT ID FROM city"
var sql2 = "SELECT A.a, B.b FROM A JOIN M ON M.a = A.a AND M.b = B.b JOIN K ON K.m = M.m;"
var sql3 = "SELECT A FROM B; SELECT B ON FROM C; select C from `a s d f g ; e $`;"

var valid_query =  `
SELECT film_text.title, first_name, last_name, rental_date, return_date
FROM localhost..rental
JOIN inventory ON inventory_id = rental.inventory_id --AND inventory.film_id = film_text.film_id
JOIN localhost..film_text ON inventory.film_id = film_text.film_id --AND film_id = *
JOIN localhost..customer ON customer_id = rental.customer_id AND address_id = address_id
;`

var psql1 = ast(sql1);
var psql2 = ast(sql2);
var psql3 = ast(sql3);

semantic(valid_query, function(err, result) {
    if (err) {
        console.error(""+ err);
        return;
    }
    console.log(result);
}, {host: "localhost", schema:"sakila"});

var nop =  0;
*/