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
// bind columns1 to columns2 (columns2 should be PK)
// columns = [column, column, ..., column]
// column: {database, table, column}
function addFkInternal(host, columns1, columns2) {
  // make three steps:
  // 1.  gather column ID's
  // 2.  check existence of FK
  // 3.  add metadata information
  var state = 0;
  function callback(err, result) {
    if (err) {
      console.error(
        `Error adding foreign key: 
        ${column1.database}.${column1.table}.${column1.column} -> ${column2.database}.${column2.table}.${column2.column}`
        );
      console.error(err)
    }
    column_ids_1 = [];
    column_ids_2 = [];
    switch(state) {
    case 0:
      // here should gather all column ID's
      state = 1;
      con.query(`
      SELECT id FROM \`column\`
      WHERE \`column\`=${column1.column}
      AND table_id=
      (
        SELECT id FROM \`table\` WHERE \`table\`=${column1.table} AND database_id=
        (
          SELECT id FROM \`database\` WHERE \`database\`=${column1.database} AND host_id=
          (
            SELECT id FROM host where host=${host}
          )
        )
      )
      `, callback);
    case 1:
      
      break;
    }
  }
}

// checks if such Foreign Key already exists in the database.
function FKExist(column_ids_1, column_ids_2, callback) {
  let L = Math.min(column_ids_1.length, column_ids_2.length);
  if (L == 0) {
    callback(false);
    return;
  }
  callback(false);
  return; // let's assume it's always false :D
  
  // algorithm:
  //  1. For every pair (column_id_1i, column_id_2i) get list of FK id's
  //  2. Find intersection of all sets, result: set of candidate FK id's
  //  3. For every foreign key id find count of containing columns
  //  4. If result of 3 contains same number, as length of column_ids, then return true, oth false

  /*
  var init_list = [
    SELECT foreign_key_id
    FROM `foreign keys`
    WHERE column1_id = ${column_ids_1[0]} AND column2_id = ${column_ids_2[0]}
  ]
  */

  for (let k = 1; k < L; k++) {
      /*
      init_list = [
        SELECT foreign_key_id
        FROM `foreign keys`
        WHERE column1_id = ${column_ids_1[k]} AND column2_id = ${column_ids_2[k]}
        AND foreign_key_id in [${init_list.join(",")]}
      ]
      */
  }

  /*
    SELECT foreign_key_id, COUNT(*) as cnt
    FROM `foreign keys`
    WHERE foreign_key_id in [${init_list.join(",")]}
    HAVING cnt = ${L}
  */
  // return true if last query is not empty
}