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
// column_group: {host, database, table, columns}
// columns = [column, column, ..., column]
// commonly, columns1 should be in table A, and columns2 should bein table B
function addFkInternal(column_group1, column_group2) {
  // make three steps:
  // 1.  gather column ID's
  // 2.  check existence of FK
  // 3.  add metadata information
  var state = 0;
  function callback(err, result) {
    if (err) {
      console.error(
        `Error adding foreign key: 
        ${column_group1.host}.${column_group1.database}.${column_group1.table} -> ${column_group2.host}.${column_group2.database}.${column_group2.table}`
        );
      console.error(err)
    }
    column_ids_1 = [];
    column_ids_2 = [];
    var ids_1_i = 0;
    var ids_2_i = 0;
    while (true) {
      switch(state) {
      case 0:
        // here should gather all column ID's
        state = 1;
        con.query(`
        SELECT id FROM \`column\`
        WHERE \`column\`=${column_group1.columns[ids_1_i]}
        AND table_id=
        (
          SELECT id FROM \`table\` WHERE \`table\`=${column_group1.table} AND database_id=
          (
            SELECT id FROM \`database\` WHERE \`database\`=${column_group1.database} AND host_id=
            (
              SELECT id FROM host where host=${column_group1.host}
            )
          )
        )
        `, callback);
      case 1:
        column_ids_1.push(result[0].id);
        ids_1_i++;
        if (ids_1_i < column_group1.columns.length) {
          state = 0;
          break;
        }
        
      case 2:
        state = 3;
        con.query(`
        SELECT id FROM \`column\`
        WHERE \`column\`=${column_group2.columns[ids_2_i]}
        AND table_id=
        (
          SELECT id FROM \`table\` WHERE \`table\`=${column_group2.table} AND database_id=
          (
            SELECT id FROM \`database\` WHERE \`database\`=${column_group2.database} AND host_id=
            (
              SELECT id FROM host where host=${column_group2.host}
            )
          )
        )
        `, callback);
      case 3:
        column_ids_2.push(result[0].id);
        ids_2_i++;
        if (ids_2_i < column_group2.columns.length) {
          state = 2;
          break;
        }
        // check if FK already exists
        state = 4;
        FKExist(column_ids_1, column_ids_2, callback);
      case 4:
        if (result)
          return; // FK already presented in database
        // otherwize, give FK unique ID, and push pairs (column_ids_1, column_ids_2)  to metatable
        // get unique FK ID
        state = 5;
        con.query(`SELECT max(foreign_key_id) + 1 as fk_id FROM \`foreign keys\``, callback);
        break;
      case 5:
        state = 6;
        var fk_id = result[0].fk_id;
        var values_to_insert = column_ids_1.map((e,i)=>
          `(0, ${fk_id}, ${e}, ${column_ids_2[i]}`
          ).join(",");
        // make insertions
        con.query(`INSERT INTO \`foreign keys\`
        (id, foreign_key_id, column1_id, column2.id)
        VALUES
        ${values_to_insert};
        `, callback);
      case 6:
        return; // that's all, values added
      }
    }
      
  }

  // making initial callback
  callback();
}

// checks if such Foreign Key already exists in the database.
function FKExist(column_ids_1, column_ids_2, callback) {
  let L = Math.min(column_ids_1.length, column_ids_2.length);
  if (L == 0) {
    callback(null, false);
    return;
  }
  
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

  /*
  for (let k = 1; k < L; k++) {
      init_list = [
        SELECT foreign_key_id
        FROM `foreign keys`
        WHERE column1_id = ${column_ids_1[k]} AND column2_id = ${column_ids_2[k]}
        AND foreign_key_id in [${init_list.join(",")]}
      ]
  }
  */

  /*
    SELECT foreign_key_id, COUNT(*) as cnt
    FROM `foreign keys`
    WHERE foreign_key_id in [${init_list.join(",")]}
    HAVING cnt = ${L}
  */
  // return true if last query is not empty

  // implement using callback async function
  var fk_list = [];
  var k = 0;
  var state = 0;
  function cb(err, result) {
    if (err) {
      callback(err, null); //an error occured, should be processed in upstream
    }
    switch(state) {
      case 0:
        // create initial query for the first pair
        con.query(`
          SELECT \`foreign_key_id\`
          FROM \`foreign keys\`
          WHERE column1_id = ${column_ids_1[0]} AND column2_id = ${column_ids_2[0]}
        `, cb);
        state = 1; // next state is for processing query
        break;
      case 1:
        fk_list = result.map(row=>row.foreign_key_id);
        k++; // next pair
        if (k < L) {
          // chaining array processing for next pairs
          con.query(`
            SELECT \`foreign_key_id\`
            FROM \`foreign keys\`
            WHERE column1_id = ${column_ids_1[k]} AND column2_id = ${column_ids_2[k]}
            AND foreign_key_id in [${fk_list.join(",")}]
          `, cb);
          // no state change needed for this operation
          break; // call again after callback
        }
        // if all pairs have been considered
        // query for the FK's that actually of size L
        con.query(`
        SELECT foreign_key_id, COUNT(*) as cnt
        FROM \`foreign keys\`
        WHERE foreign_key_id in [${init_list.join(",")}]
        HAVING cnt = ${L}
      `, cb);
        state = 2; // fall through: next state is for processing query
      case 2: 
        // check if the result is not empty, call the callback function with result
        callback (null, result.length > 0);
        break;
    }

    cb(); // initiate the phases
  }
}