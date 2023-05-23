context('Worker')
skip_if_not(
    condition = isTRUE(as.logical(Sys.getenv('WITH_REDIS', unset = 'true'))),
    message = 'Redis disabled'
)

test_that('worker', {
    connect()
    clearJobs(queue='test')
    deleteExperiment('simworkr')

    data(iris)
    data = iris
    model0 = quote(lm(Sepal.Length ~ -1 + Sepal.Width, data=data))
    model1 = quote(lm(Sepal.Length ~ Sepal.Width, data=data))

    # data-by-name, method-call
    cases = expand.grid(data='iris', method=c('model0', 'model1'))
    submitJobs('simworkr', cases, queue='test', inOrder=TRUE)
    capture.output(simworkr(queue='test', envir=environment()))
    expect_equal(jobCount(queue='test'), 0)
    expect_equal(caseCount('simworkr'), 2)
    expect_equal(getExperimentOutputs('simworkr')[['data=iris;method=model0']], eval(model0))

    # data-by-call, method-call
    irisRenamed = quote(setNames(iris, c('x1', 'x2', 'x3', 'x4', 'y')))
    modelx = quote(lm(x4 ~ x1 + x2, data=data))
    cases = expand.grid(data='irisRenamed', method='modelx')
    submitJobs('simworkr', cases, queue='test', inOrder=TRUE)
    capture.output(simworkr(queue='test', envir=environment()))
    expect_equal(jobCount(queue='test'), 0)
    expect_equal(caseCount('simworkr'), 3)

    # data-by-function, method-call
    syndataFun = function(b0, b1, sd) {
        t = 0:10
        set.seed(1)
        data.frame(t=t, y=b0 + b1 * t + rnorm(length(t), mean=0, sd=sd))
    }
    synmodel = quote(lm(y ~ t, data=data))
    cases = expand.grid(data='syndataFun', method='synmodel', b0=1, b1=1, sd=c(1, 0))
    submitJobs('simworkr', cases, queue='test', inOrder=TRUE)
    capture.output(simworkr(queue='test', envir=environment()))
    expect_equal(sigma(getExperimentOutputs('simworkr')[['b0=1;b1=1;data=syndataFun;method=synmodel;sd=1']]), .819, tolerance=.01)
    expect_equal(sigma(getExperimentOutputs('simworkr')[['b0=1;b1=1;data=syndataFun;method=synmodel;sd=0']]), 0, tolerance=.01)
    expect_equal(coef(getExperimentOutputs('simworkr')[['b0=1;b1=1;data=syndataFun;method=synmodel;sd=0']]), c('(Intercept)'=1, t=1))

    # data-by-function, method-by-function
    synmodelFun = function(data, extra) {
        lm(y ~ t, data=data)
    }
    cases = expand.grid(data='syndataFun', method='synmodelFun', b0=1, b1=1, sd=c(1, 0), extra=1)
    submitJobs('simworkr', cases, inOrder=TRUE, queue='test')
    capture.output(simworkr(queue='test', envir=environment()))
    expect_equal(coef(getExperimentOutputs('simworkr')[['b0=1;b1=1;data=syndataFun;extra=1;method=synmodelFun;sd=0']]), c('(Intercept)'=1, t=1))

    # double data-by-function with varargs, method-by-varargfunction
    syndataBaseFun = function(t, b0, b1, sd, seed, ...) {
        set.seed(seed)
        data.frame(t=t, y=b0 + b1 * t + rnorm(length(t), mean=0, sd=sd))
    }
    syndataVarFun = function(b0, b1, sd, ...) {
        syndataBaseFun(t=0:10, b0, b1, sd, ...)
    }
    synmodelVarFun = function(data, extra, ...) {
        lm(y ~ t, data=data)
    }
    cases = expand.grid(data='syndataVarFun', method='synmodelVarFun', b0=1, b1=1, sd=c(1, 0), seed=c(1,2), extra=1)
    submitJobs('simworkr', cases, inOrder=TRUE, queue='test')
    capture.output(simworkr(queue='test', envir=environment()))
    expect_equal(getExperimentOutputs('simworkr')[['b0=1;b1=1;data=syndataVarFun;extra=1;method=synmodelVarFun;sd=1;seed=1']]$model$y,
                 syndataBaseFun(t=0:10, b0=1,b1=1,sd=1, seed=1)$y)
    expect_equal(getExperimentOutputs('simworkr')[['b0=1;b1=1;data=syndataVarFun;extra=1;method=synmodelVarFun;sd=1;seed=2']]$model$y,
                 syndataBaseFun(t=0:10, b0=1,b1=1,sd=1, seed=2)$y)

    disconnect()
})
