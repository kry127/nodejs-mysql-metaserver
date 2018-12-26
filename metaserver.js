/* info

 composite fk:
 https://stackoverflow.com/questions/9780163/composite-key-as-foreign-key-sql

 how to get all fk:
 http://www.conandalton.net/2008/09/list-foreign-key-constraints-in-oracle.html
 click safely: https://www.binarytides.com/list-foreign-keys-in-mysql/+&cd=3&hl=en&ct=clnk&gl=ru
*/

var metadata_credentials = {
    host: "localhost",
    user: "root",
    password: "",
    port: 50331
  }

// connection variable
var con = null;

function connect() {
  // create connection
  con = mysql.createConnection(metadata_credentials);
  
  // connect
  con.connect(function(err, data) {
    if (err) {
      throw "Critical error: cannot establish connection with metadata server.\n" + err;
    }
  });

  // error handling
  con.on('error', function(err) {
    console.log("An error occured:\n" + err);
  })
}

function disconnect() {
  con.end();
  con = null;
}

function addHost(host) {
  con.query(`INSERT INTO \`host\` VALUES (0, ${host});`
    , function (err, result) {
      if (err) {
        console.error(`Error adding host ${host} to the metadata server.`)
        console.error(err)
      }
    });
}

function addDatabase(host, database) {
  con.query(`INSERT INTO \`database\` VALUES (0, (SELECT id FROM host where host=${host}), ${database});`
    , function (err, result) {
      if (err) {
        console.error(`Error adding database ${host}.${database} to the metadata server.`)
        console.error(err)
      }
    });
}

function addTable(host, database, table) {
  con.query(`INSERT INTO \`table\` VALUES (0, 
      (
        SELECT id FROM \`database\` WHERE \`database\`=${database} AND host_id=
        (
          SELECT id FROM host where host=${host}
        )
      )
      , ${table});`
    , function (err, result) {
      if (err) {
        console.error(`Error adding database ${host}.${database}.${table} to the metadata server.`)
        console.error(err)
      }
    });
}

function addColumn(host, database, table, column, props) {
  con.query(`INSERT INTO \`column\` VALUES (0, 
      (
        SELECT id FROM \`table\` WHERE \`table\`=${table} AND database_id=
        (
          SELECT id FROM \`database\` WHERE \`database\`=${database} AND host_id=
          (
            SELECT id FROM host where host=${host}
          )
        )
      )
      , ${column}, ${props.nullable}, ${props.type}, ${props.default});`
    , function (err, result) {
      if (err) {
        console.error(`Error adding database ${host}.${database}.${table}.${column} to the metadata server.`)
        console.error(err)
      }
    });
}

// for internally defined keys
// column1 {database, table, column, nullable, type, default}
// column2 {database, table, column, nullable, type, default}
function addFkInternal(host, column1, column2) {

}