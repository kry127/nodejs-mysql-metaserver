var mysql = require('mysql');
var fs = require('fs');
const readline = require('readline');

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

rl.prompt()
rl.on('line', (input) => {
  var inp_arr = input.split(" ")

  if (inp_arr[0].startsWith("get")) {
    if (inp_arr.length <  2) {
      console.error("Insufficient number of arguments. Type help.");
      console.log("  get <server> [user] [password] [port]")
      rl.prompt();
      return;
    }
    var credential = {};
    credential.host = inp_arr[1];
    credential.user = inp_arr[2] || "anonymous";
    credential.password = inp_arr[3] || "";
    if (false || inp_arr[4])
      credential.port = new Integer(inp_arr[4]);

    session(credential);
    rl.pause(); // pause rl exec

  } else if (inp_arr[0].startsWith("help")) {
    console.log("Available commands: \n")
    console.log("  get <server> [user] [password] [port]")
    // should be:
    //console.log("  get <server>[:user[:password[:port]]] [.db[.table]]")
  } else {
    console.error(`Unknown command ${input.split(' ')[0]}`)
  }
  rl.prompt();
});


function session(credential) {
  var con = mysql.createConnection(credential);
    
  // how to query constraints: https://stackoverflow.com/questions/4004205/show-constraints-on-tables-command
  con.connect(function(err) {
      if (err) throw err;
      console.log("Connected!");
      
    // use information schema DB
    con.query("USE information_schema;", function (err, result) {
      if (err) throw err;
      // select all databases name
      con.query("SELECT * FROM SCHEMATA;", function (err, result) {
        if (err) throw err;
        schemas = result.map(e=>e.SCHEMA_NAME)
        schemas_plain_str = schemas.join()
        console.log("Result: " + schemas_plain_str);
        // next step -- extract all tables name
        con.query("SELECT * FROM TABLES LIMIT 1000;", function (err, result) {
          if (err) throw err;
          console.log("Result: " + result);
          // the last step -- get all columns of the tables
          con.query("SELECT * FROM COLUMNS;", function (err, result) {
            if (err) throw err;
            console.log("Result: " + result);
            //then close the connection
            con.end();
            rl.resume(); // resume readline
          });
        });
      });
    });
  });

  
}