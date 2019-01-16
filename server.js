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
  let _str_get_help = `
  get <server>:[user]:[password]:[port][/db[/table]]
  select <column1> [,<column2>[,...[,<columnn>]]] from <table1> join <table2> on <columni>=<columnj>`;

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

// this function begins retrieval metadata from server
function session(credential, db, table) {
  var con = mysql.createConnection(credential);
  
  let qse = Object.freeze( {
    connection: 0,
    connected: 1,
    add_host: 2,
    use_schema: 3,
    get_schemas: 4,
    add_schemas: 5,
    get_tables_prep: 6,
    get_tables: 7,
    add_tables: 8,
    get_columns_prep: 9,
    get_columns: 10,
    add_columns: 11,
    get_fk_def_prep: 12,
    get_fk_def: 13,
    get_fk_prep: 14,
    get_fk: 15,
    add_fk: 16,
    cycle_analyze: 17
  })
  var query_state = qse.connection;
  /* the idea of variables "schemas", "tables" and "columns":
   * the function query_state_machine_callback will be set as universal callback for every
   * session query results. This includes querying for tables and for fields.
   * So, the idea is: modified state machine, that analyses upcoming tables sequentially.
   * Once, the 1 db 1 tbl processed till last state (get_columns), the index is incrementing,
   * state is restoring to get_schemas or get_tables and automata starts over again.
   * The strategy is similar to LR-analyzer with stack machine.
   */
  function Iterator() {
    this.arr = [];
    this.index = 0;
    this.current = function() {
      return this.arr[this.index]
    }
  }
  var schemas = new Iterator();
  var tables = new Iterator();
  var columns = new Iterator();
  var fk_defs = new Iterator();

  function query_state_machine_callback(err, result) {
    if (err) {
      console.error(err); // that's all
      return;
    }
    switch (query_state) {
    case qse.connection: //connection
      console.log("Connected to specified host! Connecting to metadata server...");
      // try to connect to metadata server
      metasrv.connect(query_state_machine_callback);
      query_state = qse.connected;
      break;
    case qse.connected:
      console.log("Connected!")
      // host can be added here
      metasrv.addHost(credential, query_state_machine_callback)
      query_state=qse.add_host
      break;
    case qse.add_host: // when host added -- continue
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
          console.error(`Error: schema ${db} is not presented in list :(`);
          return;
        } else {
          schemas.arr=[db] // left only db to analyze
        }
      } else {
        // filter with tabu schemas
        schemas.arr = schemas.arr.filter(scheme=>tabu.indexOf(scheme) == -1)
      }
    
    case qse.add_schemas:
      // here we can add each element of schemas.arr to metadata database
      metasrv.addSchema({host: credential.host, schema: schemas.current()}, query_state_machine_callback)
      query_state=qse.get_tables_prep
      break;
    case qse.get_tables_prep: // when schema added -- continue

      query_state = qse.get_tables; // next step -- form query for getting tables
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
    case qse.add_tables:
      // here we can add each element of tables.arr to metadata database
      metasrv.addTable(
        {host: credential.host, schema: schemas.current(), table: tables.current()},
        query_state_machine_callback
      )
      query_state=qse.get_columns_prep
      break;
    case qse.get_columns_prep: // when table added -- continue
      query_state = qse.get_columns; // next step is aquiring all columns
      con.query(`SELECT * FROM COLUMNS WHERE TABLE_SCHEMA='${schemas.current()}'
                AND TABLE_NAME='${tables.current()}';`
        , query_state_machine_callback);
      break;
    case qse.get_columns:
      columns.index = 0;
      columns.arr = result.map(e=>
        {
          return {
                // full qualifier
                host: credential.host,
              schema: schemas.current(),
               table: tables.current(),
                // column itself
              column: e.COLUMN_NAME,
            nullable: e.IS_NULLABLE,
                type: e.DATA_TYPE,
             default: e.COLUMN_DEFAULT
          }
        });
      var res = columns.arr.map(e=>e.column).join()
      console.log("Columns: " + res);

      query_state = qse.add_columns
    case qse.add_columns:
      // here we can add each element of columns.arr to metadata database
      metasrv.addColumn(columns.current(), query_state_machine_callback)
      query_state = qse.get_fk_def_prep
      break;
    case qse.get_fk_def_prep: // when column added -- continue
      // there should be queries for key constraints for EACH column
      var col = columns.current();
      query_state = qse.get_fk_def;
      con.query(`
      select distinct
      -- defines fk: constraint_name + table_schema
      constraint_name,
      table_schema
      from
          information_schema.key_column_usage
      where
          referenced_table_name is not null
      and 
      (
            table_schema = '${col.schema}'
        AND table_name   = '${col.table}'
        AND column_name  = '${col.column}'
        OR
            referenced_table_schema = '${col.schema}'
        AND referenced_table_name   = '${col.table}'
        AND referenced_column_name  = '${col.column}'
      );
      `
        , query_state_machine_callback);
      break;
    case qse.get_fk_def:
      if (result.length == 0) {
        query_state = qse.cycle_analyze; // nothing to do here
        query_state_machine_callback(); // call again
        break;
      }
      // here we've got all foreign key definitions, now for each we need full set of keys
      fk_defs.index = 0;
      fk_defs.arr = result.map(e=>{
        return {
          constraint_name: e.constraint_name,
          table_schema: e.table_schema
        }
      });
      query_state = qse.get_fk_prep;
    case qse.get_fk_prep:
      // then for each foreign key definition retrieve full connection
      var fk_def = fk_defs.current();
      query_state = qse.get_fk;
      con.query(`
      select
        table_schema, table_name, column_name,
        referenced_table_schema, referenced_table_name, referenced_column_name
      from
          information_schema.key_column_usage
      where
          referenced_table_name is not null
      and constraint_name = '${fk_def.constraint_name}'
      and table_schema = '${fk_def.table_schema}';
      `
        , query_state_machine_callback);
      break;
    case qse.get_fk:
      // getting result and checking, that ALL table_schema, table_name, referenced_table_schema and
      // referenced_column_name has exact same value (not in normal form 2)
      var good = result.every((v,i,a)=>
        v.table_schema == a[0].table_schema
        && v.table_name == a[0].table_name
        && v.referenced_table_schema == a[0].referenced_table_schema
        && v.referenced_table_name == a[0].referenced_table_name
      )
      if (!good) {
        console.error(`Error: dispersed foreign key ${connection.host}.${schemas.current()}.${fk_def.constraint_name}.`)
        console.error("The key contain multiple pairs from different tables, schemas and hosts, check information_schema.")
        query_state = qse.cycle_analyze; // ignore error and continue key iteration anyway
        query_state_machine_callback(); // call again
        break;
      }
      if (result.length == 0) {
        console.error(`Error: foreign key ${connection.host}.${schemas.current()}.${fk_def.constraint_name} is empty.`)
        query_state = qse.cycle_analyze; // ignore error and continue key iteration anyway
        query_state_machine_callback(); // call again
        break;
      }
      // if all good, then we can add FK to metadata database
      var column_group_1 = {
        host: credential.host,
        schema: result[0].table_schema,
        table: result[0].table_name,
        columns: result.map(row=>row.column_name)
      }
      var column_group_2 = {
        host: credential.host,
        schema: result[0].referenced_table_schema,
        table: result[0].referenced_table_name,
        columns: result.map(row=>row.referenced_column_name)
      }
      // then, finally, can add to metadata database
      metasrv.addFK(column_group_1, column_group_2, query_state_machine_callback);
      query_state = qse.add_fk; // don't forget next state
      break;
    case qse.add_fk:
      // well, if query ok, we actually need to analyze next keys, so... fall through
      query_state = qse.cycle_analyze
    case qse.cycle_analyze:
      // reverse check
      if (fk_defs.arr.length > fk_defs.index + 1) {
        fk_defs.index++;
        query_state = qse.get_fk_prep; // get ready to select constraints of each selected column
        query_state_machine_callback(); //cycling
        break;
      }
      if (columns.arr.length > columns.index + 1) {
        columns.index++;
        query_state = qse.add_columns; // get ready to select constraints of each selected column
        query_state_machine_callback(); //cycling
        break;
      }
      if (tables.arr.length > tables.index + 1) {
        tables.index++;
        query_state = qse.add_tables; // get ready to select colums of next tables
        query_state_machine_callback(); //cycling
        break;
      }
      if (schemas.arr.length > schemas.index + 1) {
        schemas.index++;
        query_state = qse.add_schemas; // get ready to select tables of next db
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