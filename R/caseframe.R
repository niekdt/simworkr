#' @export
#' @title Generate case names for the given case settings
#' @inheritParams submitJobs
generateCaseNames = function(cases, columns=names(cases)) {
    assert_that(is.data.frame(cases))

    if(nrow(cases) == 0) {
        return(character(0))
    }

    cases = data.table(cases)[, columns, with=FALSE]
    setcolorder(cases, order(names(cases))) #sort by column name
    casemat = sapply(cases, as.character) %>% t
    namemat = mapply(paste, rep(names(cases), nrow(cases)), casemat, MoreArgs=list(sep='='), USE.NAMES=FALSE) %>%
        matrix(nrow=length(cases), ncol=nrow(cases))
    apply(namemat, 2, paste, collapse=';')
}


addCaseSettings = function(dt_cases, dt_cases2) {
    stopifnot(!any(names(dt_cases2) %in% names(dt_cases)))

    data.table(dt_cases[rep(1:.N, nrow(dt_cases2))], dt_cases2[rep(1:.N, each=nrow(dt_cases))])
}