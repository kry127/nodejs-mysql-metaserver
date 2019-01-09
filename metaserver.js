/* info

 composite fk:
 https://stackoverflow.com/questions/9780163/composite-key-as-foreign-key-sql

 how to get all fk:
 http://www.conandalton.net/2008/09/list-foreign-key-constraints-in-oracle.html
 click safely: https://www.binarytides.com/list-foreign-keys-in-mysql/+&cd=3&hl=en&ct=clnk&gl=ru
*/


module.exports = {
  // connection
  connect: connect,
  disconnect: disconnect,
  // addition
  addHost: addHost,
  addDatabase: addDatabase,
  addTable: addTable,
  addColumn: addColumn,
  addFK: addFK,
  // check
  checkFK: checkFK,
  // testing
  test: test
}

var mysql = require('mysql');

var metadata_credentials = {
    host: "localhost",
    user: "root",
    password: "",
    port: 50331
  }

// connection variable
var con = null;

function connect(callback) {
  // create connection
  con = mysql.createConnection(metadata_credentials);
  
  var block = 1;
  // connect
  con.connect(function(err, data) {
    if (err) {
      throw "Critical error: cannot establish connection with metadata server.\n" + err;
    }
    // automatically  use database
    con.query("USE metadata;", function(err, data) {
      if (err) {
        throw "Critical error: cannot switch to metadata schema.\n" + err;
      }
      if (typeof callback === "function")
        callback(err, data);
    });
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

// data : {host}
function addHost(data, callback) {
  con.query(`INSERT INTO \`host\` VALUES (0, '${data.host}');`
    , function (err, result) {
      if (err) {
        if (err.errno == 1062) {// 'code: ER_DUP_ENTRY
          console.error(`Host ${data.host} already exist`)
          if (typeof callback === "function")
            return callback(null, result);
        }
        console.error(`Error adding database ${data.host} to the metadata server.`)
      }
      if (typeof callback === "function")
         return callback(err, result);
    });
}

// data: {host, database}
function addDatabase(data, callback) {
  con.query(`INSERT INTO \`database\` VALUES (0,
    (
      SELECT id FROM host where host='${data.host}'
    )
    , '${data.database}');`
    , function (err, result) {
      if (err) {
        if (err.errno == 1062) {// 'code: ER_DUP_ENTRY
          console.error(`Schema ${data.host}.${data.database} already exist`)
          if (typeof callback === "function")
            return callback(null, result);
        }
        console.error(`Error adding database ${data.host}.${data.database} to the metadata server.`)
      }
      if (typeof callback === "function")
         return callback(err, result);
    });
}

// data: {host, database, table}
function addTable(data, callback) {
  con.query(`INSERT INTO \`table\` VALUES (0, 
      (
        SELECT id FROM \`database\` WHERE \`database\`='${data.database}' AND host_id=
        (
          SELECT id FROM host where host='${data.host}'
        )
      )
      , '${data.table}');`
    , function (err, result) {
      if (err) {
        
        if (err.errno == 1062) {// 'code: ER_DUP_ENTRY
          console.error(`Table ${data.host}.${data.database}.${data.table} already exist`)
          if (typeof callback === "function")
            return callback(null, result);
        }
        console.error(`Error adding database ${data.host}.${data.database}.${data.table} to the metadata server.`)
      }
      if (typeof callback === "function")
        return callback(err, result);
    });
}

// data: {host, database, table, column, nullable, type, default}
function addColumn(data, callback) {
  con.query(`INSERT INTO \`column\` VALUES (0, 
      (
        SELECT id FROM \`table\` WHERE \`table\`='${data.table}' AND database_id=
        (
          SELECT id FROM \`database\` WHERE \`database\`='${data.database}' AND host_id=
          (
            SELECT id FROM host where host='${data.host}'
          )
        )
      )
      , '${data.column}', '${data.nullable}', '${data.type}', '${data.default}');`
    , function (err, result) {
      if (err) {
        if (err.errno == 1062) {// 'code: ER_DUP_ENTRY
          console.error(`Column ${data.host}.${data.database}.${data.table}.${data.column} already exist`)
          if (typeof callback === "function")
            return callback(null, result);
        }
        console.error(`Error adding column ${data.host}.${data.database}.${data.table}.${data.column} to the metadata server.`)

      }
      if (typeof callback === "function")
        return callback(err, result);
    });
}

// for internally defined keys
// bind columns1 to columns2 (columns2 should be PK)
// column_group: {host, database, table, columns}
// columns = [column, column, ..., column]
// commonly, columns1 should be in table A, and columns2 should bein table B
function addFK(column_group1, column_group2, callback) {
  // make three steps:
  // 1.  gather column ID's
  // 2.  check existence of FK
  // 3.  add metadata information
  var state = 0;
  column_ids_1 = [];
  column_ids_2 = [];
  var ids_1_i = 0;
  var ids_2_i = 0;
  function cb(err, result) {
    if (err) {
      console.error(
        `Error adding foreign key: 
        ${column_group1.host}.${column_group1.database}.${column_group1.table} -> ${column_group2.host}.${column_group2.database}.${column_group2.table}`
        );
        if (typeof callback === "function")
          return callback(err, result)
    }
    while (true) {
      switch(state) {
      case 0:
        // here should gather all column ID's
        state = 1;
        con.query(`
        SELECT id FROM \`column\`
        WHERE \`column\`='${column_group1.columns[ids_1_i]}'
        AND table_id=
        (
          SELECT id FROM \`table\` WHERE \`table\`='${column_group1.table}' AND database_id=
          (
            SELECT id FROM \`database\` WHERE \`database\`='${column_group1.database}' AND host_id=
            (
              SELECT id FROM host where host='${column_group1.host}'
            )
          )
        )
        `, cb);
        return;
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
        WHERE \`column\`='${column_group2.columns[ids_2_i]}'
        AND table_id=
        (
          SELECT id FROM \`table\` WHERE \`table\`='${column_group2.table}' AND database_id=
          (
            SELECT id FROM \`database\` WHERE \`database\`='${column_group2.database}' AND host_id=
            (
              SELECT id FROM host where host='${column_group2.host}'
            )
          )
        )
        `, cb);
        return;
      case 3:
        column_ids_2.push(result[0].id);
        ids_2_i++;
        if (ids_2_i < column_group2.columns.length) {
          state = 2;
          break;
        }
        // check if FK already exists
        state = 4;
        checkFK(column_ids_1, column_ids_2, cb);
        return;
      case 4:
        if (result) {
           // FK already presented in database
          console.error(
          "Foreign key: " + 
          `${column_group1.host}.${column_group1.database}.${column_group1.table}.[${column_group1.columns.join(" ")}] -> ${column_group2.host}.${column_group2.database}.${column_group2.table}.[${column_group2.columns.join(" ")}]`
          +" already exist")
          if (typeof callback === "function")
            return callback(null, result)
        }
        // otherwize, give FK unique ID, and push pairs (column_ids_1, column_ids_2)  to metatable
        // get unique FK ID
        state = 5;
        con.query(`SELECT max(foreign_key_id) + 1 as fk_id FROM \`foreign keys\``, cb);
        return;
      case 5:
        state = 6;
        var fk_id = result[0].fk_id || 1; // if no ids presented, use 1 instead
        var values_to_insert = column_ids_1.map((e,i)=>
          `(0, ${fk_id}, '${e}', '${column_ids_2[i]}')`
          ).join(",");
        // make insertions
        con.query(`INSERT INTO \`foreign keys\`
        (id, foreign_key_id, column1_id, column2_id)
        VALUES
        ${values_to_insert};
        `, cb);
        return;
      case 6:
        if (typeof callback === "function")
          return callback(null, result) // that's all, values added
      }
    }
      
  }

  // making initial callback
  cb();
}

// checks if such Foreign Key already exists in the database.
function checkFK(column_ids_1, column_ids_2, callback) {
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
      return;
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
            AND foreign_key_id in (${(fk_list.length)?fk_list.join(","):"NULL"})
          `, cb);
          // no state change needed for this operation
          break; // call again after callback
        }
        // if all pairs have been considered
        // query for the FK's that actually of size L
        con.query(`
        SELECT foreign_key_id, COUNT(*) as cnt
        FROM \`foreign keys\`
        WHERE foreign_key_id in (${(fk_list.length)?fk_list.join(","):"NULL"})
        GROUP BY foreign_key_id
        HAVING cnt = ${L}
      `, cb);
        state = 2; // next state is for processing query
        break;
      case 2: 
        // check if the result is not empty, call the callback function with result
        callback (null, result.length > 0);
        return;
    }
  }

  cb(); // initiate the phases
}

function test() {
  connect(function(err, data) {
    // define stuff
    var H1 = "LOL1";
    var H2 = "LOL2";
    var DB1 = "DB1";
    var DB2 = "DB2";
    var T1 = "tbl1";
    var T2 = "tbl2";
    var C1 = "col1";
    var C2 = "col2";
    
    var step = 0;
    // make all async through callback "cb"
    function cb(err, result) {
      if (err) {
        throw err; // no need to escalate further
      }
      switch (++step) {
        // add some hosts (2)
        case 1: addHost({host:H1}, cb); break;
        case 2: addHost({host:H2}, cb); break;
        case 3: addHost({host:H1}, cb); break; // add again for lulz
        // add some databases (2*2=4)
        case 4: addDatabase({host:H1, database: DB1}, cb); break;
        case 5: addDatabase({host:H1, database: DB2}, cb); break;
        case 6: addDatabase({host:H2, database: DB2}, cb); break;
        case 7: addDatabase({host:H2, database: DB1}, cb); break;
        case 8: addDatabase({host:H1, database: DB2}, cb); break; // add again for lulz
        // add some tables (2*2*2=8)
        case 9: addTable({host:H1, database: DB1, table: T1}, cb); break;
        case 10: addTable({host:H1, database: DB1, table: T2}, cb); break;
        case 11: addTable({host:H1, database: DB2, table: T2}, cb); break;
        case 12: addTable({host:H1, database: DB2, table: T1}, cb); break;
        case 13: addTable({host:H2, database: DB2, table: T1}, cb); break;
        case 14: addTable({host:H2, database: DB2, table: T2}, cb); break;
        case 15: addTable({host:H2, database: DB1, table: T2}, cb); break;
        case 16: addTable({host:H2, database: DB1, table: T1}, cb); break;
        case 17: addTable({host:H2, database: DB2, table: T2}, cb); break; // add again for lulz
        // add some columns (2*2*2*2=16)
        case 18: addColumn({host:H1, database: DB1, table: T1, column: C1}, cb); break;
        case 19: addColumn({host:H1, database: DB1, table: T1, column: C2}, cb); break;
        case 20: addColumn({host:H1, database: DB1, table: T2, column: C2}, cb); break;
        case 21: addColumn({host:H1, database: DB1, table: T2, column: C1}, cb); break;
        case 22: addColumn({host:H1, database: DB2, table: T2, column: C1}, cb); break;
        case 23: addColumn({host:H1, database: DB2, table: T2, column: C2}, cb); break;
        case 24: addColumn({host:H1, database: DB2, table: T1, column: C2}, cb); break;
        case 25: addColumn({host:H1, database: DB2, table: T1, column: C1}, cb); break;
        case 26: addColumn({host:H2, database: DB2, table: T1, column: C1}, cb); break;
        case 27: addColumn({host:H2, database: DB2, table: T1, column: C2}, cb); break;
        case 28: addColumn({host:H2, database: DB2, table: T2, column: C2}, cb); break;
        case 29: addColumn({host:H2, database: DB2, table: T2, column: C1}, cb); break;
        case 30: addColumn({host:H2, database: DB1, table: T2, column: C1}, cb); break;
        case 31: addColumn({host:H2, database: DB1, table: T2, column: C2}, cb); break;
        case 32: addColumn({host:H2, database: DB1, table: T1, column: C2}, cb); break;
        case 33: addColumn({host:H2, database: DB1, table: T1, column: C1}, cb); break;
        case 34: addColumn({host:H2, database: DB1, table: T1, column: C1}, cb); break; // add again for lulz
        // add some FK (16*16*6=)
        // six for one of 16*16 variants
        case 35:
        addFK({host: H1, database: DB1, table: T1, columns:[C1]}
        ,{host: H2, database: DB2, table: T2, columns:[C1]}, cb);
        break;
        case 36:
        addFK({host: H1, database: DB1, table: T1, columns:[C2]}
        ,{host: H2, database: DB2, table: T2, columns:[C2]}, cb);
        break;
        case 37:
        addFK({host: H1, database: DB1, table: T1, columns:[C1]}
        ,{host: H2, database: DB2, table: T2, columns:[C2]}, cb);
        break;
        case 38:
        addFK({host: H1, database: DB1, table: T1, columns:[C2]}
        ,{host: H2, database: DB2, table: T2, columns:[C1]}, cb);
        break;
        case 39:
        addFK({host: H1, database: DB1, table: T1, columns:[C1, C2]}
        ,{host: H2, database: DB2, table: T2, columns:[C1, C2]}, cb);
        break;
        case 40:
        addFK({host: H1, database: DB1, table: T1, columns:[C2, C1]}
        ,{host: H2, database: DB2, table: T2, columns:[C1, C2]}, cb);
        break;
        // the thing is: this could be vice-versa, so it is not 16*16/2
        // it could be reflective, so it is not 16*15, and it is even not both 16*15/2 binomial either
        
        // repeat some column for lulz
        case 41:
        addFK({host: H1, database: DB1, table: T1, columns:[C2]}
          ,{host: H2, database: DB2, table: T2, columns:[C1]}, cb); // should be error
        break;
        // reflect it for checking mirroring
        case 42:
        addFK({host: H2, database: DB2, table: T2, columns:[C1]}
          ,{host: H1, database: DB1, table: T1, columns:[C2]}, cb);
        break;
        // make reflective connection on the table itself
        case 43:
        addFK({host: H1, database: DB1, table: T1, columns:[C2]}
          ,{host: H1, database: DB1, table: T1, columns:[C2]}, cb);
        break;
        // make it more stupid :D
        case 44:
        addFK({host: H1, database: DB1, table: T1, columns:[C1, C2]}
          ,{host: H1, database: DB1, table: T1, columns:[C2, C1]}, cb); // don't make any sense
        break;
        // try to add it again for lulz
        case 45:
        addFK({host: H1, database: DB1, table: T1, columns:[C1, C2]}
          ,{host: H1, database: DB1, table: T1, columns:[C2, C1]}, cb); // don't make any sense too
        break;
        // protection from the idiot: duplicate pair twice
        case 46:
        addFK({host: H1, database: DB2, table: T2, columns:[C1, C1]}
          ,{host: H2, database: DB1, table: T1, columns:[C2, C2]}, cb);
        break;     
      }
      // that's all for tests
    }
    cb(); // launch the chain!
  });
}
