#' @export
#' @importFrom R.utils withTimeout doCall printf
#' @importFrom methods formalArgs
#' @importFrom utils modifyList
#' @title Run a simulation worker
#' @inheritParams connect
#' @param name Worker name. If not specified, a name is generated.
#' @param maxJobs The maximum number of jobs that this worker is allowed to fetch from the queue.
#' @param catchError Whether to catch computation errors and continue.
#' @param envir The environment from which to retrieve the data and method. Note that the data and method calls are evaluated in isolation.
simworkr = function(host=getOption('redis.host', 'localhost'),
                    port=getOption('redis.port', 6379),
                    password=getOption('redis.pwd'),
                    name=NULL,
                    queue='',
                    maxJobs=Inf,
                    catchError=FALSE,
                    envir=parent.frame()) {
    lastContext = redisGetContext()
    if(is.null(name)) {
        if(requireNamespace('uuid')) {
            name = uuid::UUIDgenerate(use.time=FALSE)
        } else {
            name = paste(LETTERS[sample(1:26, 10)], collapse = '')
        }
    }
    assert_that(is.scalar(name), is.character(name))
    assert_that(is.scalar(queue), is.character(queue))
    assert_that(is.number(maxJobs), maxJobs > 0)

    printf(': Starting worker "%s"\n', name)
    printf('  Datetime: %s\n', Sys.time())
    printf('  Working directory: "%s"\n', getwd())
    printf('  Platform: %s\n', .Platform$OS.type)

    connect(host=host, port=port, password=password)
    printf('  Active on job queue "%s"\n', queue)

    successKey = jobqueue_success_key(queue)
    fetchCount = 0

    while(fetchCount < maxJobs && jobCount(queue=queue) > 0) {
        printf('\n\n= Next job\n')
        fetchCount = fetchCount + 1
        jobdata = nextJob(queue=queue)
        if(is.null(jobdata)) {
            warning('Got a job with NULL content. Trying a new job...', immediate.=TRUE)
            next()
        }

        expName = jobdata$experiment
        expKey = experimentKey(expName)
        caseName = jobdata$caseName
        fullCase = jobdata$case
        dataVar = as.character(fullCase$data)
        methodVar = as.character(fullCase$method)
        case = fullCase[setdiff(names(fullCase), c('data', 'method'))]
        assert_that(!is.null(caseName))
        assert_that(!is.null(dataVar), exists(dataVar, envir=envir))
        assert_that(!is.null(methodVar), exists(methodVar, envir=envir))
        printf(': Evaluating job of experiment %s...\n: Case: %s\n', expName, caseName)

        if(isCaseEvaluated(expName, caseName)) {
            printf(': Experiment "%s", case "%s": already evaluated. Redo...\n', expName, caseName)
        }

        pushError = function(e) {
            err = list()
            err$caseName = caseName
            err$case = case
            err$e = e
            redisLPush(jobqueue_errors_key(queue), err)
            warning(e)
        }

        dataObj = get(dataVar, envir=envir)

        if(is.call(dataObj)) {
            data = condTryCatch(
                cond=catchError,
                expr=eval(dataObj, envir=case),
                error=function(e) {
                    printf('! Error occurred while evaluating data expression: "%s"\n', e$message)
                    pushError(e)
                    return(NULL)
                }
            )
        } else if(is.function(dataObj)) {
            data = condTryCatch(
                cond=catchError,
                expr=doCall(dataObj, args=case, .ignoreUnusedArgs=not('...' %in% formalArgs(dataObj))),
                error=function(e) {
                    printf('! Error occurred while calling data function: "%s"\n', e$message)
                    pushError(e)
                    return(NULL)
                }
            )
        } else {
            data = dataObj
        }

        if(is.null(data)) {
            next
        }

        caseEnv = modifyList(case, list(data=data, .experiment = expName, .case = caseName))

        method = get(methodVar, envir=envir)
        assert_that(is.call(method) || is.function(method))

        if(is.call(method)) {
            out = condTryCatch(
                cond=catchError,
                expr=eval(method, envir=caseEnv),
                error=function(e) {
                    printf('! Error occurred while evaluating method expression: "%s"\n', e$message)
                    pushError(e)
                    return(NULL)
                }
            )
        } else {
            out = condTryCatch(
                cond=catchError,
                expr=doCall(method, args=caseEnv, .ignoreUnusedArgs=not('...' %in% formalArgs(method))),
                error=function(e) {
                    printf('! Error occurred while calling method function: "%s"\n', e$message)
                    pushError(e)
                    return(NULL)
                }
            )
        }

        if(is.null(out)) {
            next
        }

        caseResult = list(case=case, output=out)
        redisHSet(expKey, caseName, caseResult)
        printf('Case results submitted.\n')
        redisIncr(successKey)
    }

    printf('= No jobs left.\n')
    withTimeout(redisClose(), timeout=5)
    printf('= Goodbye.\n')

    redisSetContext(lastContext)
}