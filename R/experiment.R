#' @title Experiment key
#' @description Get the Redis key of the experiment
#' @param exp Name of the experiment
experimentKey = function(exp) {
    assert_that(is.character(exp))
    sprintf('experiment-%s', exp)
}

#' @export
#' @title Tests whether the experiment exists
isExperiment = function(exp) {
    assert_that(isConnected())
    sapply(experimentKey(exp), redisExists)
}

#' @export
#' @title Get the names of all experiments
getExperimentNames = function() {
    assert_that(isConnected())
    keys = unlist(redisKeys('experiment-*'))
    pat = 'experiment-(.+)'
    sub(pat, '\\1', keys[grepl(pat, keys)])
}

#' @export
#' @title Get the raw experiment result structure
#' @param subset Optional logical expression or case-name regex pattern for filtering cases
#' @param include Whether to include matching cases in the output, or exclude
#' @param ... Ignored.
#' @return A list of the raw experiment result structures of the matching cases
getExperimentResults = function(exp,
    subset = '*',
    include = TRUE,
    ...) {
    subsetCall = substitute(subset)

    assert_that(
        isConnected(),
        is.character(subsetCall) || is.call(subsetCall),
        is.flag(include)
    )
    key = experimentKey(exp)

    if (!redisExists(key)) {
        warning(sprintf('Experiment "%s" not found', exp))
        list()
    }

    if (is.character(subsetCall)) {
        if (subset == '*') {
            redisHGetAll(key)
        } else {
            caseNames = getCaseNames(exp)
            caseMask = grepl(subset, caseNames) == include
            redisHMGet(key, caseNames[caseMask])
        }
    } else {
        expData = redisHGetAll(key)

        allCasesTable = lapply(expData, '[[', 'case') %>%
            rbindlist(fill = TRUE)

        dtIdx = allCasesTable[, `:=`(.ROW_INDEX, .I)] %>%
            base::subset(subset = eval(subsetCall))

        matchIdx = dtIdx$.ROW_INDEX
        if (!include) {
            matchIdx = -matchIdx
        }

        expData[matchIdx]
    }
}

#' @export
#' @title Get a list of all method outputs per case
#' @inheritDotParams getExperimentResults
getExperimentOutputs = function(exp, ...) {
    assert_that(isExperiment(exp))
    results = getExperimentResults(exp, ...)
    lapply(results, '[[', 'output')
}


#' @export
#' @title Rename an experiment
renameExperiment = function(name, newname) {
    assert_that(isExperiment(name))
    key = experimentKey(name)
    newkey = experimentKey(newname)

    stopifnot(redisExists(key))
    if (redisExists(newkey)) {
        stop('Cannot rename experiment. New name already in use. Try experiment_merge')
    } else {
        redisRename(key, newkey)
        message('Successfully renamed experiment.')
    }
}

#' @export
#' @title Merge an experiment into another
#' @inheritParams caseCount
#' @param exp2 The experiment to merge into \code{exp}
mergeExperiment = function(exp, exp2, pattern = '*') {
    assert_that(isConnected())
    key = experimentKey(exp)
    addkey = experimentKey(exp2)
    case_names = experiment_caseNames(exp2) %>% .[grep(pattern, .)]
    newcase_names = setdiff(case_names, experiment_caseNames(exp))
    message(sprintf(
        'Adding %d new cases to experiment %s.',
        length(newcase_names),
        exp
    ))
    lapply(newcase_names, function(newcase)
        redisHSet(key, newcase, redisHGet(addkey, newcase)))
}

#' @export
#' @title Delete an experiment and its results
deleteExperiment = function(exp) {
    key = experimentKey(exp)
    if (redisExists(key)) {
        redisDelete(key)
        message(sprintf('Deleted results of experiment "%s".', exp))
    }
    else {
        message(sprintf('Nothing to delete for experiment "%s".', exp))
    }
}




#' @importFrom memoise memoise
#' @title Evaluate result with case data
#' @description Evaluate the result with access to the case-related dataset
#' @param resultFun A function with arguments `(tsdata, output)`
experiment_computeResultWithData = function(name,
    resultfun,
    verbose = 1,
    dataArgs = c(
        'data',
        'dataseed',
        'numgroups',
        're',
        'numobs',
        'numtraj',
        'noise',
        'propnoise',
        'int.sd',
        'slo.sd',
        'q.sd',
        'props'
    )) {
    assert_that(isConnected())
    key = experimentKey(name)
    if (!redisExists(key)) {
        stop(sprintf('No results for experiment "%s"', name))
    }

    results = redisHGetAll(key)
    case_names = experiment_caseNames(name)

    create_dataset = function(...) {
        args = list(...)
        do.call(args$data %>% as.character, args)
    }
    datafun = memoise(create_data) # cached function

    out = vector('list', length(results))
    for (i in seq(4400, length(results))) {
        if (verbose && i %% verbose == 0) {
            messagef('[%d/%d] "%s"...', i, length(results), case_names[i])
        }
        store = results[[i]]
        tsdata = do.call(datafun, store$case[dataArgs])
        out[[i]] = resultfun(tsdata, store$output)
    }
    return(out)
}


#' @title Get post-processed output table
#' @param outfun optional function that is called on the output of each field, for returning additional scalar columns
# e.g. outfun = function(x) c(Jaccard=x$Jaccard)
experiment_getOutputTableOf = function(name, outfun) {
    assert_that(isConnected())
    results = experiment_getResults(name)
    cases = lapply(results, '[[', 'case') %>% lapply(as.data.table)
    dt_cases = do.call(rbind, c(cases, fill = TRUE))
    if (!missing(outfun)) {
        outlist = lapply(results, '[[', 'output') %>% lapply(outfun) %>% lapply(function(x)
            as.data.table(as.list(x)))
        outframe = rbindlist(outlist, fill = TRUE)
        stopifnot(nrow(outframe) == nrow(dt_cases))
        dt_cases = cbind(dt_cases, outframe)
    }
    setorderv(dt_cases, names(dt_cases))
    setkey(dt_cases, model)
    return(dt_cases)
}

experiment_getOutputTable = function(name) {
    assert_that(isConnected())
    results = experiment_getResults(name)

    dt_cases = lapply(results, '[[', 'case') %>%
        lapply(as.data.table) %>%
        rbindlist(fill = TRUE)

    outlist = lapply(results, '[[', 'output')
    fields = c(
        'start',
        'time',
        'converged',
        'WRSS',
        'trendWRSS',
        'silhouette',
        'refSilhouette',
        'BIC',
        'ARI',
        'Mirkin',
        'sj1',
        'sj2',
        'sjMin',
        'sjTotal',
        'VI',
        'KL',
        'Dunn',
        'refDunn',
        'emptyClusters',
        'loneClusters'
    )
    dt_main = lapply(outlist, '[', fields) %>%
        lapply(as.data.table) %>%
        rbindlist(fill = TRUE)

    dtout = cbind(dt_cases, dt_main)
    return(dtout)
}