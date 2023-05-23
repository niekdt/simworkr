# tryCatch statement conditional on the first argument
condTryCatch = function(cond, expr, error) {
    if(cond) {
        tryCatch(expr=expr, error=error)
    } else {
        eval(expr)
    }
}