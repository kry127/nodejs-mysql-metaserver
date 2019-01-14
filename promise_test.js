// test promises
// https://stackoverflow.com/questions/36547292/use-promise-to-process-mysql-return-value-in-node-js
// let's assume we have terrible function, that randomly calls back with result
function async_func(callback) {
    var timeout =  Math.floor(Math.random()*500)
    setTimeout(function() {
        if (typeof callback === "function") {
            callback("success")
        }
    }, timeout)
}

async_func(function(msg) {
    console.error(msg);
});
// so we can't use it's result properly here  :(

// try Promises to resolve this issue
function sync() {
    // create new Promise
    return new Promise(
        // for callback we use function, that accepts functions "resolve" and "reject"
        function(resolve, reject) {
            // in this callback we make a call to async function
            async_func(function(msg) {
                // instead of action, we call resolve
                resolve(msg);
            })
        }
    );
}

sync().then(function(val) {
    console.log(val);
    return sync()
}).then(function(val) { // then chaining
    console.log(val);
    return sync()
}).then(function(val) {
    console.log(val);
    return sync()
})