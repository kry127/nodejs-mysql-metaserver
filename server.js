var mysql = require('mysql');
var fs = require('fs');
const readline = require('readline');
var metasrv = require('./metaserver');

// how to make sync nodejs mysql:
// https://html5hive.org/node-js-quickies-working-with-mysql/
// https://code.tutsplus.com/tutorials/managing-the-asynchronous-nature-of-nodejs--net-36183

// interface for readline
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: 'mymetasql> '
});

//https://www.ensembl.org/info/data/mysql.html
// should be stored on a server?
var credentials = [
  {
    host: "ensembldb.ensembl.org",
    user: "anonymous",
    password: ""
  },
  {
    host: "localhost",
    user: "root",
    password: ""
  },
]

// these schemas are not analyzed implicitly
var tabu = ["information_schema", "sys", "performance_schema"]

rl.prompt()
rl.on('line', (input) => {
  let _str_get_help = "  get <server>:[user]:[password]:[port][/db[/table]]";

  var inp_arr = input.split(" ")

  if (inp_arr[0].startsWith("get")) {
    var get_args = inp_arr.slice(1).join(' ');
    if (get_args.length == 0) {
      console.error("Insufficient number of arguments. Type help.");
      console.log(_str_get_help)
      rl.prompt();
      return;
    }
    var get_dot_split = get_args.split('/');
    var get_credential = get_dot_split[0].split(':');
    var credential = {};
    credential.host = get_credential[0];
    credential.user = get_credential[1] || "anonymous";
    credential.password = get_credential[2] || "";
    credential.port = get_credential[3] ? new Integer(get_credential[3]) : 3306;

    var db = get_dot_split[1]
    var table = get_dot_split[2]

    session(credential, db, table);
    rl.pause(); // pause rl exec
    return;

  } else if (inp_arr[0].startsWith("help")) {
    console.log("Available commands: \n")
    console.log(_str_get_help);
  } else {
    console.error(`Unknown command ${input.split(' ')[0]}`)
  }
  rl.prompt();
});


function session(credential, db, table) {
  var con = mysql.createConnection(credential);
  
  let qse = Object.freeze( {
    connection: 0,
    use_schema: 1,
    get_schemas: 2,
    get_tables_prep: 3,
    get_tables: 4,
    get_columns_prep: 5,
    get_columns: 6,
    cycle_analyze: 7
  })
  var query_state = qse.connection;
  /* the idea of variables "schemas" and "tables":
   * the function query_state_machine_callback will be set as universal callback for every
   * session query results. This includes querying for tables and for fields.
   * So, the idea is: modified state machine, that analyses upcoming tables sequentially.
   * Once, the 1 db 1 tbl processed till last state (get_columns), the index is incrementing,
   * state is restoring to get_schemas or get_tables and automata starts over again.
   * The strategy is similar to LR-analyzer with stack machine.
   */
  var schemas = {
    arr: [],
    index: 0,
    current: function() {
      return this.arr[this.index];
    }
  };
  var tables = {
    arr: [],
    index: 0,
    current: function() {
      return this.arr[this.index];
    }
  };
  function query_state_machine_callback(err, result) {
    if (err) {
      console.error(err); // that's all
      return;
    }
    switch (query_state) {
    case qse.connection: //connection
      console.log("Connected!");
      
      // use information schema DB
      query_state = qse.use_schema;
      con.query("USE information_schema;", query_state_machine_callback);
      break;
    case qse.use_schema:
      query_state = qse.get_schemas;
      con.query("SELECT * FROM SCHEMATA;", query_state_machine_callback);
      break;
    case qse.get_schemas:
      schemas.index = 0;
      schemas.arr = result.map(e=>e.SCHEMA_NAME)
      schemas_plain_str = schemas.arr.join()
      console.log("Schemas: " + schemas_plain_str);
      // if db var present, check if it is presented in scheme
      if (db) {
        if (schemas.arr.indexOf(db) == -1) {
          console.error(`Error: database ${db} is not presented in list :(`);
          return;
        } else {
          schemas.arr=[db] // left only db to analyze
        }
      } else {
        // filter with tabu schemas
        schemas.arr = schemas.arr.filter(scheme=>tabu.indexOf(scheme) == -1)
      }
    case qse.get_tables_prep:
    query_state = qse.get_tables;
      con.query(`SELECT * FROM TABLES WHERE TABLE_SCHEMA='${schemas.current()}';`, query_state_machine_callback);
      break;
    case qse.get_tables:
      tables.index = 0;
      tables.arr = result.map(e=>e.TABLE_NAME)
      schemas_plain_str = tables.arr.join()
      console.log("Tables: " + schemas_plain_str);
      
      // if table var present, check if it is presented in scheme
      if (table) {
        if (tables.arr.indexOf(table) == -1) {
          // I think it's ok, if there is no tables appeared
          query_state = qse.cycle_analyze; // go to cycle analyze immediately
          query_state_machine_callback(); //cycling
          return;
        } else {
          tables.arr=[table] // left only table to analyze
        }
      }
    case qse.get_columns_prep:
    query_state = qse.get_columns;
      con.query(`SELECT * FROM COLUMNS WHERE TABLE_SCHEMA='${schemas.current()}'
                AND TABLE_NAME='${tables.current()}';`
        , query_state_machine_callback);
      break;
    case qse.get_columns:
      var cols_info = result.map(e=>
        {
          return {
              column: e.COLUMN_NAME,
            nullable: e.IS_NULLABLE,
                type: e.DATA_TYPE,
             default: e.COLUMN_DEFAULT
          }
        });
      var res = cols_info.map(e=>e.column).join()
      console.log("Columns: " + res);
      // there should be queries for key constraints

      query_state = qse.cycle_analyze
    case qse.cycle_analyze:
      // reverse check
      if (tables.arr.length > tables.index + 1) {
        tables.index++;
        query_state = qse.get_columns_prep; // get ready to select colums of next tables
        query_state_machine_callback(); //cycling
        break;
      }
      if (schemas.arr.length > schemas.index + 1) {
        schemas.index++;
        query_state = qse.get_tables_prep; // get ready to select tables of next db
        query_state_machine_callback(); //cycling
        break;
      }
      // queries ended successfully
      con.end();
      console.log("end :)");
      rl.resume(); // resume readline
      rl.prompt();
      break;
    }
  }

  con.connect(query_state_machine_callback);
  
}

//session(credentials[1]);