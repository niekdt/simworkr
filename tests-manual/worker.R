library(simworkr)

model = function(...) {
    Sys.sleep(2)
    return(TRUE)
}

simworkr(queue='test', catchError=TRUE, envir=environment())
