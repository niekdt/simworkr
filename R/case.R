#' @export
#' @title Get the evaluated case settings in a data.frame
#' @inheritParams experimentKey
#' @inheritDotParams getExperimentResults
getCases = function(exp, ...) {
    results = getExperimentResults(exp, ...)

    if(length(results) == 0) {
        message(sprintf('No case results for experiment "%s"', exp))
        return(data.frame())
    } else {
        caseSettings = lapply(results, '[[', 'case')
        rbindlist(l=caseSettings, fill=TRUE)
    }
}


#' @export
#' @title Get the names of the evaluated cases
getCaseNames = function(exp) {
    assert_that(isConnected())
    key = experimentKey(exp)
    if(redisExists(key)) {
        return(unlist(redisHFields(key)))
    } else {
        return(character())
    }
}


#' @export
#' @title Count the number of cases in an experiment
#' @description Count the number of cases that match the given pattern. Useful to check before running a deletion
#' @param pattern Case name filter
caseCount = function(exp, pattern='*') {
    case_names = getCaseNames(exp)
    length(case_names[grep(pattern, case_names)])
}

#' @export
#' @title Check whether the given cases have been evaluated
#' @param caseNames Character vector of case names
isCaseEvaluated = function(exp, caseNames) {
    assert_that(is.character(caseNames) || is.factor(caseNames))
    as.character(caseNames) %in% getCaseNames(exp)
}

#' @export
#' @title Delete cases of experiment according to a pattern
#' @description Delete cases from the experiment that match the given name pattern
#' @inheritParams caseCount
#' @param sim Whether to simulate the deletion, showing the number of cases that would have been deleted.
#' @param fixed Whether to match the given pattern as-is
deleteCases = function(exp, pattern, sim=TRUE, fixed = FALSE) {
    assert_that(
        is.character(pattern),
        length(pattern) > 0,
        is.flag(sim),
        is.flag(fixed)
    )

    key = experimentKey(exp)
    if (redisExists(key)) {
        case_names = redisHFields(key) %>% unlist

        if (fixed) {
            del_cases = intersect(case_names, pattern)
        } else {
            del_cases = case_names[grep(pattern, case_names, fixed = fixed)]
        }

        if (length(del_cases) == 0) {
            message(sprintf('No cases for experiment "%s" match pattern "%s", nothing to delete.', exp, pattern))
        } else {
            if (sim) {
                message(sprintf('This would delete %d cases from experiment "%s", with pattern "%s". Run again with sim=FALSE to actually delete the cases.', length(del_cases), exp, pattern))
            } else {
                foreach(field=del_cases) %do% {redisHDel(key, field)}
                message(sprintf('Deleted %d cases from experiment "%s", with pattern "%s"', length(del_cases), exp, pattern))
            }
        }
    } else {
        message(sprintf('No results for experiment "%s", nothing to delete.', exp))
    }
}