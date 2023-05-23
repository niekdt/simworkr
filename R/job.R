queueKey = function(queue) {
    paste('jobs', queue, sep='-')
}

jobqueue_errors_key = function(queue) {
    paste('errors', queue, sep='-')
}

jobqueue_success_key = function(queue) {
    paste('success', queue, sep='-')
}

getJobExperimentName = function(jobname) {
    pat = '^(.+?)-.+'
    sub(pat, '\\1', jobname[grepl(pat, jobname)])
}

getJobCaseName = function(jobname) {
    pat = '^.+?-(.+)'
    sub(pat, '\\1', jobname[grepl(pat, jobname)])
}

#' @export
#' @title Get the number of open jobs
jobCount = function(queue='') {
    redisLLen(queueKey(queue))
}

#' @title Test whether a the given job has already been queued
isJobQueued = function(exp, caseNames, queue='') {
    jobqKey = queueKey(queue)
    if(redisExists(jobqKey)) {
        ls_jobdata = redisLRange(jobqKey, 0, redisLLen(jobqKey)-1)
        expmask = sapply(ls_jobdata, '[[', 'experiment') == exp
        job_names = sapply(ls_jobdata[expmask], '[[', 'caseName')
        return(caseNames %in% job_names)
    } else {
        return(rep(FALSE, length(caseNames)))
    }
}

#' @export
#' @title Clear the job queue
clearJobs = function(queue='') {
    jobqname = queueKey(queue)
    if(redisExists(jobqname)) {
        redisDelete(jobqname)
        if(nchar(queue) == 0) {
            message('Job queue cleared.')
        } else {
            message(sprintf('Job queue "%s" cleared.', queue))
        }
    } else {
        message('Nothing to clear. No jobs scheduled.')
    }
}

#' @export
#' @title Clear all job errors
#' @inheritParams submitJobs
clearJobErrors = function(queue='') {
    redisDelete(jobqueue_errors_key(queue=queue))
}

#' @export
#' @title Monitor job progress
#' @aliases jobs
#' @inheritParams submitJobs
jobMonitor = function(queue='') {
    errorskey = jobqueue_errors_key(queue=queue)
    successkey = jobqueue_success_key(queue=queue)
    cat(sprintf('%d open jobs\n', jobCount(queue=queue)))
    cat('Hit <Esc> to stop monitoring.\n')
    redisSet(successkey, charToRaw('0'))
    total_count = jobCount(queue=queue)
    if(redisExists(errorskey)) {
        start_err_count = redisLLen(errorskey)
    } else {
        start_err_count = 0
    }
    last_success_count = 0
    last_err_count = start_err_count
    timekeeping = NULL

    while(jobCount(queue=queue) > 0 || as.integer(redisInfo()$connected_clients) > 1L) {
        new_success_count = redisGet(successkey) %>% as.integer
        new_err_count = redisLLen(errorskey)
        if(last_success_count != new_success_count) {
            if(is.null(timekeeping)) {
                timekeeping = list(start=Sys.time(), startcount=new_success_count)
                sec_per_case = 0
            } else {
                elapsed_time = as.numeric(Sys.time() - timekeeping$start, 'secs')
                sec_per_case = elapsed_time / (new_success_count - timekeeping$startcount)
            }
            printf(strftime(Sys.time(), '%b-%d %H:%M:%S |'))
            printf('%6s  %-10s %4d errors %6.1f s/case,  %4d min left [%d workers]\n',
                   paste0(round(new_success_count/total_count*100, 1), '%'),
                   paste0('(', new_success_count, '/', total_count, ')'),
                   new_err_count - start_err_count,
                   round(sec_per_case,1),
                   round((total_count - new_success_count) * sec_per_case / 60),
                   as.integer(redisInfo()$connected_clients)-1L)
            last_success_count = new_success_count
        }
        if(new_err_count != last_err_count) {
            errors = redisLRange(errorskey, last_err_count, new_err_count-1)
            print(errors)
            last_err_count = new_err_count
        }

        Sys.sleep(1)
    }
    cat('Ended monitoring because no other clients are connected.')
}

#' @export
jobs = jobMonitor

#' @export
#' @inheritParams submitJobs
#' @aliases jobq
jobQueue = function(queue='') {
    message(sprintf('Retrieving overview of %d jobs...', jobCount(queue=queue)))
    jobs = redisLRange(queueKey(queue=queue), 0, jobCount(queue=queue))
    expseq = sapply(jobs, '[[', 'experiment')
    exprle = rle(expseq)
    data.table(Experiment=exprle$values, Jobs=exprle$lengths)
}

#' @export
#' @title Get active job queues
getJobQueues = function() {
    queues = redisKeys('jobs*')
    substr(queues, 6, nchar(queues))
}

#' @export
jobq = jobQueue


#' @export
#' @title Submit a series of cases as jobs
#' @param exp Name of the experiment to which the cases belong.
#' @param cases A \code{data.frame} containing different parameter settings per row. A "data" and "method" column are required, containing a character or factor denoting the name of the variable to evaluate.
#' @param inOrder Whether to submit the cases in order. Shuffling the order of the cases results in more reliable time estimates.
#' @param renew Whether to redo cases that have already been evaluated.
#' @param queue The name of the queue to which the jobs will be submitted.
submitJobs = function(exp, cases, inOrder=FALSE, renew=FALSE, queue='') {
    assert_that(isConnected())
    assert_that(is.scalar(exp), is.character(exp))
    assert_that(is.scalar(queue), is.character(queue))
    assert_that(is.data.frame(cases))
    assert_that(has_name(cases, 'data'))
    assert_that(has_name(cases, 'method'))
    assert_that(is.character(cases$data) || is.factor(cases$data))
    assert_that(is.character(cases$method) || is.factor(cases$method))
    dataMask = sapply(as.character(unique(cases$data)), exists, envir=parent.frame())
    assert_that(all(dataMask), msg=paste('cannot find datasets/calls:', paste(names(dataMask)[!dataMask], collapse=', ')))
    methodMask = sapply(as.character(unique(cases$method)), exists, envir=parent.frame())
    assert_that(all(methodMask), msg=paste('cannot find methods:', paste(names(methodMask)[!methodMask], collapse=', ')))

    key = experimentKey(exp)
    jobqKey = queueKey(queue)

    message(sprintf(': Received %d cases for experiment "%s" to evaluate.', nrow(cases), exp))
    caseNames = generateCaseNames(cases)
    if(renew) {
        caseMask = rep(TRUE, length(caseNames))
    } else {
        message(': Identifying new cases...')
        caseMask = !isCaseEvaluated(exp, caseNames)
    }

    message(': Checking queued jobs...')
    jobMask = !isJobQueued(exp, caseNames, queue=queue)

    # select new cases to be submitted
    newCases = cases[caseMask & jobMask,]
    newCaseNames = generateCaseNames(newCases)
    job_names = paste(key, newCases, sep='-')

    if(nrow(newCases) == 0) {
        message(': No new jobs to submit.')
        return(invisible(NULL))
    }

    # Submit jobs
    queueStr = ifelse(nchar(queue) > 0, sprintf('to queue "%s"', queue), '')
    if(inOrder) {
        message(sprintf(': Submitting %d jobs %s...', nrow(newCases), queueStr))
        case_indices = seq_len(nrow(newCases))
    } else {
        message(sprintf(': Submitting %d jobs in random order %s...', nrow(newCases), queueStr))
        case_indices = seq_len(nrow(newCases)) %>% sample()
    }

    for(i in case_indices) {
        case = newCases[i,] %>% as.list
        jobdata = list(
            experiment = exp,
            caseName = newCaseNames[i],
            case = case
        )
        redisLPush(jobqKey, jobdata)
    }
    message('= Done.')
}

#' @title Fetch the next job
#' @return NULL when no more jobs are available
nextJob = function(queue='') {
    redisRPop(queueKey(queue))
}